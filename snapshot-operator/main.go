// Copyright 2021 dfuse Platform Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"go.uber.org/zap"

	"github.com/streamingfast/logging"
)

var flagGCPProject = flag.String("gcp-project", "", "GCP project where to create disks and do all the things")

var zlog *zap.Logger

func init() {
	logging.ApplicationLogger("snapshotter", "github.com/streamingfast/snapshotter/snapshot-operator", &zlog)
}

func main() {
	flag.Parse()

	op, err := newOperator(*flagGCPProject)
	noError(err, "new operator")

	err = op.Run(context.Background())
	noError(err, "run")
}

func noError(err error, message string, args ...interface{}) {
	if err != nil {
		quit(message+": "+err.Error(), args...)
	}
}

func quit(message string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, message+"\n", args...)
	os.Exit(1)
}
