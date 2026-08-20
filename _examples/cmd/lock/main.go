package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"time"

	"cirello.io/dynamolock/v5"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/urfave/cli/v3"
)

func main() {
	log.SetPrefix("lock: ")
	log.SetFlags(0)
	app := &cli.Command{
		HideVersion: true,
		Name:        "lock",
		Usage:       "lock and execute given command",
		Flags: []cli.Flag{
			&cli.BoolFlag{Name: "release-on-error", Aliases: []string{"r"}},
			&cli.BoolFlag{Name: "wait-for-lock", Aliases: []string{"w"}},
			&cli.StringFlag{
				Name:  "table",
				Value: "locks",
			},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			lockName := c.Args().First()
			if lockName == "" {
				return errors.New("missing lock name")
			}
			cmd := c.Args().Tail()
			if len(cmd) == 0 {
				return errors.New("missing command")
			}
			tableName := c.String("table")
			ctx, stop := signal.NotifyContext(ctx, os.Interrupt)
			defer stop()
			client, err := dialDynamoDB(ctx, tableName)
			if err != nil {
				return err
			}
			if err := createTable(ctx, client, tableName); err != nil {
				return err
			}
			lock, err := grabLock(ctx, client, lockName, c.Bool("wait-for-lock"))
			if err != nil {
				return err
			}
			return runCommand(ctx, lock, c.Bool("release-on-error"), cmd)
		},
	}
	if err := app.Run(context.Background(), os.Args); err != nil {
		log.Fatal(err)
	}
}

func dialDynamoDB(ctx context.Context, tableName string) (*dynamolock.Client, error) {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("cannot load AWS configuration: %w", err)
	}
	client, err := dynamolock.New(
		dynamodb.NewFromConfig(cfg),
		tableName,
		dynamolock.WithLeaseDuration(3*time.Second),
		dynamolock.WithHeartbeatPeriod(1*time.Second),
		dynamolock.WithPartitionKeyName("key"),
	)
	if err != nil {
		return nil, fmt.Errorf("cannot start dynamolock client: %w", err)
	}
	return client, nil
}

func createTable(ctx context.Context, client *dynamolock.Client, tableName string) error {
	_, err := client.CreateTable(ctx, tableName,
		dynamolock.WithProvisionedThroughput(&types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(5),
			WriteCapacityUnits: aws.Int64(5),
		}),
		dynamolock.WithCustomPartitionKeyName("key"),
	)
	if err != nil {
		var errResourceInUse *types.ResourceInUseException
		if !errors.As(err, &errResourceInUse) {
			return fmt.Errorf("cannot create dynamolock client table: %w", err)
		}
	}
	return nil
}

func grabLock(ctx context.Context, client *dynamolock.Client, lockName string, wait bool) (*dynamolock.Lock, error) {
	for {
		lock, err := client.AcquireLock(ctx, lockName, dynamolock.WithDeleteLockOnRelease())
		if err != nil {
			if wait && ctx.Err() == nil {
				continue
			}
			return nil, fmt.Errorf("cannot lock %s: %w", lockName, err)
		}
		return lock, nil
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
			if errLock := lock.Close(context.Background()); errLock != nil {
				log.Println("cannot release lock after failure:", errLock)
			}
		}
		return fmt.Errorf("error: %w", err)
	}
	if errLock := lock.Close(context.Background()); errLock != nil {
		log.Println("cannot release lock after completion:", errLock)
	}
	return nil
}
