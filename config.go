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

package snapshotter

import (
	"fmt"
	"os"
	"strings"
)

var EnvConfig = newConfig()

type Config struct {
	project   string
	namespace string
	podName   string
}

func (c *Config) Valid() error {
	var missingConfigEnv []string

	if c.project == "" {
		missingConfigEnv = append(missingConfigEnv, "PROJECT")
	}

	if c.namespace == "" {
		missingConfigEnv = append(missingConfigEnv, "NAMESPACE")
	}

	//	if c.podName == "" {
	//		missingConfigEnv = append(missingConfigEnv, "HOSTNAME")
	//	}

	if len(missingConfigEnv) > 0 {
		return fmt.Errorf("missing those env variable(s): %s", strings.Join(missingConfigEnv, ", "))
	}

	return nil
}

func newConfig() *Config {
	c := &Config{
		project:   os.Getenv("PROJECT"),
		namespace: os.Getenv("NAMESPACE"),
		podName:   os.Getenv("HOSTNAME"),
	}
	return c
}
