package main

import (
	"context"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"time"

	"cirello.io/dynamolock"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/urfave/cli"
	"golang.org/x/xerrors"
)

func main() {
	log.SetPrefix("lock: ")
	log.SetFlags(0)
	app := cli.NewApp()
	app.HideVersion = true
	app.Name = "lock"
	app.Usage = "lock and execute given command"
	app.Flags = []cli.Flag{
		cli.BoolFlag{Name: "release-on-error,r"},
		cli.BoolFlag{Name: "wait-for-lock,w"},
		cli.StringFlag{
			Name:  "table",
			Value: "locks",
		},
	}
	app.Action = func(c *cli.Context) error {
		lockName := c.Args().First()
		if lockName == "" {
			return xerrors.New("missing lock name")
		}
		cmd := c.Args().Tail()
		if len(cmd) == 0 {
			return xerrors.New("missing command")
		}
		tableName := c.String("table")
		client, err := dialDynamoDB(tableName)
		if err != nil {
			return err
		}
		if err := createTable(client, tableName); err != nil {
			return err
		}
		lock, err := grabLock(client, lockName, c.Bool("wait-for-lock"))
		if err != nil {
			return err
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		trap := make(chan os.Signal, 1)
		signal.Notify(trap, os.Interrupt)
		go func() {
			<-trap
			cancel()
		}()
		return runCommand(ctx, lock, c.Bool("release-on-error"), cmd)
	}
	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}

func dialDynamoDB(tableName string) (*dynamolock.Client, error) {
	session, err := session.NewSession()
	if err != nil {
		return nil, xerrors.Errorf("cannot create AWS session: %w", err)
	}
	client, err := dynamolock.New(
		dynamodb.New(session),
		tableName,
		dynamolock.WithLeaseDuration(3*time.Second),
		dynamolock.WithHeartbeatPeriod(1*time.Second),
		dynamolock.WithPartitionKeyName("key"),
	)
	if err != nil {
		return nil, xerrors.Errorf("cannot start dynamolock client: %w", err)
	}
	return client, nil
}

func createTable(client *dynamolock.Client, tableName string) error {
	_, err := client.CreateTable(tableName,
		dynamolock.WithProvisionedThroughput(&dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(5),
			WriteCapacityUnits: aws.Int64(5),
		}),
		dynamolock.WithCustomPartitionKeyName("key"),
	)
	if err != nil {
		var awsErr awserr.RequestFailure
		isTableAlreadyCreatedError := xerrors.As(err, &awsErr) && awsErr.StatusCode() == 400 && awsErr.Message() == "Cannot create preexisting table"
		if !isTableAlreadyCreatedError {
			return xerrors.Errorf("cannot create dynamolock client table: %w", err)
		}
	}
	return nil
}

func grabLock(client *dynamolock.Client, lockName string, wait bool) (*dynamolock.Lock, error) {
	for {
		lock, err := client.AcquireLock(lockName, dynamolock.WithDeleteLockOnRelease())
		if err != nil && wait {
			continue
		} else if err != nil {
			return nil, xerrors.Errorf("cannot lock %s: %w", lockName, err)
		}
		return lock, err
	}
}

func runCommand(ctx context.Context, lock *dynamolock.Lock, releaseOnError bool, cmd []string) error {
	command := cmd[0]
	var parameters []string
	if len(cmd) > 1 {
		parameters = cmd[1:]
	}
	wrappedCommand := exec.CommandContext(ctx, command, parameters...)
	wrappedCommand.Stdin = os.Stdin
	wrappedCommand.Stdout = os.Stdout
	wrappedCommand.Stderr = os.Stderr
	if err := wrappedCommand.Run(); err != nil {
		if releaseOnError {
			log.Println("errored, releasing lock")
			if lockErr := lock.Close(); lockErr != nil {
				log.Println("cannot release lock after failure:", lockErr)
			}
		}
		return xerrors.Errorf("error: %w", err)
	}
	if lockErr := lock.Close(); lockErr != nil {
		log.Println("cannot release lock after completion:", lockErr)
	}
	return nil
}
