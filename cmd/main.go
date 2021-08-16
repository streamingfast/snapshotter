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
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/streamingfast/logging"
	"github.com/streamingfast/snapshotter"
	"google.golang.org/api/compute/v1"
)

type config struct {
	namespace string
	region    string
	version   string
	force     bool
}

var zlog *zap.Logger

func init() {
	logging.ApplicationLogger("snapshotter", "github.com/streamingfast/snapshotter/cmd", &zlog)
}

func main() {
	ctx := context.Background()
	config := &config{
		namespace: os.Getenv("NAMESPACE"),
		region:    envOr("REGION", "us-central1"),
		version:   envOr("VERSION", "geth-v1"),
		force:     os.Getenv("FORCE") == "true",
	}

	if config.namespace == "" {
		fmt.Println("the NAMESPACE environment variable must be set")
		return
	}

	resnap := []string{
		// Bring your own list of snapshots
	}

	createVolumesFromList(ctx, config, resnap)
}

func list(ctx context.Context, config *config) {
	if err := snapshotter.EnvConfig.Valid(); err != nil {
		log.Fatalln("unable to boostrap operation", err)
	}

	prefix := fmt.Sprintf("%s-%s", config.namespace, config.version)
	snapshots, err := listSnapshots(ctx, prefix)
	if err != nil {
		log.Fatalln("unable to list snapshots", err)
	}

	for _, el := range snapshots {
		fmt.Println(el.Name)
	}
}

func take(ctx context.Context, config *config) {
	err := snapshotter.TakeSnapshotFromEnv(ctx, snapshotter.GenerateName(config.namespace, config.namespace, uint32(time.Now().Unix())))
	if err != nil {
		log.Fatalln("create snapshot:", err)
	}
}

func createVolumesForMatchingPrefix(ctx context.Context, config *config, prefix string, interval int) {
	if err := snapshotter.EnvConfig.Valid(); err != nil {
		log.Fatalln(err)
	}

	snapshotPrefix := fmt.Sprintf("%s-%s-%s", config.namespace, config.version, prefix)
	candidateSnapshots, err := listSnapshots(ctx, snapshotPrefix)
	if err != nil {
		log.Fatalln("unable to list snapshots", err)
	}

	var snapshots []*compute.Snapshot
	for i, candidateSnapshot := range candidateSnapshots {
		// Takes first element, at <interval> and last one, or all elements if interval is -1 (means all)
		if interval == -1 || i == 0 || i%interval == 0 || i == len(candidateSnapshots)-1 {
			snapshots = append(snapshots, candidateSnapshot)
		}
	}

	if len(snapshots) == 0 {
		log.Fatalln(fmt.Errorf("no snapshots found for prefix %s at interval %d", snapshotPrefix, interval))
	}

	t0 := time.Now()
	createVolumes(ctx, config, snapshots)
	fmt.Fprintf(os.Stderr, "Time elapsed: %s\n", time.Since(t0))
}

func createVolumesFromList(ctx context.Context, config *config, snapshotSuffixes []string) {
	if err := snapshotter.EnvConfig.Valid(); err != nil {
		log.Fatalln(err)
	}

	snapshotPrefix := fmt.Sprintf("%s-%s", config.namespace, config.version)
	candidateSnapshots, err := listSnapshots(ctx, snapshotPrefix)
	if err != nil {
		log.Fatalln("unable to list snapshots", err)
	}

	isMatchingSnapshot := func(snapshot *compute.Snapshot) bool {
		for _, snapshotSuffix := range snapshotSuffixes {
			if strings.HasSuffix(snapshot.Name, snapshotSuffix) {
				return true
			}
		}

		return false
	}

	var snapshots []*compute.Snapshot
	for _, candidateSnapshot := range candidateSnapshots {
		if isMatchingSnapshot(candidateSnapshot) {
			snapshots = append(snapshots, candidateSnapshot)
		}
	}

	if len(snapshots) != len(snapshotSuffixes) {
		log.Fatalln(fmt.Errorf("snapshot suffixes [%s] not fully resolved, only %d resolved", strings.Join(snapshotSuffixes, ", "), len(snapshots)))
	}

	t0 := time.Now()
	createVolumes(ctx, config, snapshots)

	fmt.Fprintf(os.Stderr, "Time elapsed: %s\n", time.Since(t0))
}

func createVolumes(ctx context.Context, config *config, snapshots []*compute.Snapshot) {
	var alternate int

	wg := sync.WaitGroup{}
	for _, el := range snapshots {
		alternate = (alternate + 1) % 2
		wg.Add(1)

		// "us-central1-b" and "us-central1-c" are the two zones where we have nodes
		_ = alternate
		zone := string('c') // + alternate)
		el := el
		go func() {
			err := createVolume(ctx, config, el, zone)
			if err != nil {
				fmt.Printf("ERROR unable to create volume for snapshot %s in zone %s: %s\n", el.Name, zone, err)
			}

			wg.Done()
		}()
	}

	wg.Wait()
	return
}

func createVolume(ctx context.Context, config *config, snapshot *compute.Snapshot, zone string) error {
	snapshotZone := config.region + "-" + zone
	if !config.force {
		fmt.Printf("Would create volume %s in zone %s\n", snapshot.Name, snapshotZone)
		return nil
	}

	disk, err := snapshotter.InsertPVFromSnapshot(ctx, zlog, snapshot, "", snapshotZone)
	if err != nil {
		return err
	}

	fmt.Printf(`
apiVersion: v1
kind: PersistentVolume
metadata:
  name: pv-%[1]s
spec:
  storageClassName: ""
  capacity:
    storage: %[2]dGi
  accessModes:
  - ReadWriteOnce
  gcePersistentDisk:
    pdName: %[1]s
    fsType: ext4
  persistentVolumeReclaimPolicy: Delete
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: pvc-%[1]s
spec:
  storageClassName: ""
  volumeName: pv-%[1]s
  accessModes:
  - ReadWriteOnce
  resources:
    requests:
      storage: %[2]dGi
---
`, disk.Name, disk.SizeGb)
	return nil
}

func listSnapshots(ctx context.Context, prefix string) (out []*compute.Snapshot, err error) {
	snapshots, err := snapshotter.ListSnapshots(ctx)
	if err != nil {
		return nil, err
	}

	for _, snapshot := range snapshots {
		if strings.HasPrefix(snapshot.Name, prefix) {
			out = append(out, snapshot)
		}
	}

	return
}

func envOr(key, defaultIfEmpty string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultIfEmpty
	}

	return value
}
