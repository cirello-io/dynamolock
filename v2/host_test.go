/*
Copyright 2019 github.com/ucirello

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package dynamolock_test

import (
	"os"
	"strings"
)

// DynamoTestHost allows overriding host used for tests using DYNAMODB_LOCAL_HOST environment
// variable (default is http://localhost:8000).
func DynamoTestHost() string {
	host := os.Getenv("DYNAMODB_LOCAL_HOST")
	if host == "" {
		host = "http://localhost:8000"
	}
	// remove trailing / since also used as raw TCP host/port (by stripping http://)
	host = strings.TrimSuffix(host, "/")
	return host
}
