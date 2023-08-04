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
	"context"
	"fmt"
	"os/exec"
	"strings"
	"time"

	"go.uber.org/zap"
	"google.golang.org/api/compute/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

func ListSnapshots(ctx context.Context) (out []*compute.Snapshot, err error) {
	service, err := compute.NewService(ctx)
	if err != nil {
		return
	}

	// List all snapshots
	lst, err := service.Snapshots.List(EnvConfig.project).Do()
	if err != nil {
		return
	}

	out = lst.Items
	return
}

func InsertPVFromSnapshot(ctx context.Context, logger *zap.Logger, snapshot *compute.Snapshot, namePrefix, zone string) (out *compute.Disk, err error) {
	service, err := compute.NewService(ctx)
	if err != nil {
		return
	}

	pdName := "batch-" + namePrefix + snapshot.Name

	logger.Info("launching creation of persistent disk", zap.String("name", pdName), zap.String("zone", zone))

	_, err = service.Disks.Insert(EnvConfig.project, zone, &compute.Disk{
		Description:    "created by snapshotter, from " + snapshot.Name,
		Name:           pdName,
		SourceSnapshot: snapshot.SelfLink,
		Type:           "projects/" + EnvConfig.project + "/zones/" + zone + "/diskTypes/pd-ssd",
	}).Do()
	if err != nil {
		return
	}

	for {
		disk, err := service.Disks.Get(EnvConfig.project, zone, pdName).Do()
		if err != nil {
			return nil, err
		}

		if disk.Status == "READY" {
			logger.Info("disk creation ready", zap.String("name", disk.Name), zap.String("status", disk.Status), zap.Int64("size_gb", disk.SizeGb))
			return disk, nil
		}

		time.Sleep(10 * time.Second)
	}
}

func GenerateName(namespace, appNameVer string, lastSeenBlockNum uint32) string {
	return fmt.Sprintf("%s-%s-%0.10d", namespace, appNameVer, lastSeenBlockNum)
}

func TakeSnapshotFromEnv(ctx context.Context, snapshotName string) error {
	return TakeSnapshot(ctx, snapshotName, EnvConfig.project, EnvConfig.namespace, EnvConfig.podName, "", EnvConfig.archive)
}

func TakeSnapshot(ctx context.Context, snapshotName, project, namespace, pod, prefix string, archive bool) error {
	pd, err := getPersistentDisk(ctx, pod, namespace, prefix)
	if err != nil {
		return fmt.Errorf("error getting persistent disk: %v", err)
	}

	err = exec.CommandContext(ctx, "/bin/sync").Start()
	if err != nil {
		return fmt.Errorf("/bin/sync: %s", err)
	}

	time.Sleep(10 * time.Second)

	err = createSnapshot(ctx, snapshotName, project, pd, archive)

	time.Sleep(10 * time.Second)

	return err
}

func createSnapshot(ctx context.Context, snapshotName, project string, pd *pdDef, archive bool) error {
	service, err := compute.NewService(ctx)
	if err != nil {
		return err
	}

	theSnapshot := &compute.Snapshot{
		Name: snapshotName,
		//Description: "some snapshot attempt",
		StorageLocations: []string{
			pd.region,
		},
	}

	if archive {
		theSnapshot.SnapshotType = "ARCHIVE"
	} else {
		theSnapshot.SnapshotType = "STANDARD"
	}

	op, err := service.Disks.CreateSnapshot(project, pd.zone, pd.name, theSnapshot).Do()
	if err != nil {
		return err
	}

	fmt.Printf("Snapshot creation: %s %s %s\n", op.Status, op.SelfLink, op.Zone)

	return nil
}

type pdDef struct {
	name   string
	zone   string
	region string
}

func getPersistentDisk(ctx context.Context, pod, namespace, prefix string) (out *pdDef, err error) {
	config, err := rest.InClusterConfig()
	if err == rest.ErrNotInCluster {
		config = &rest.Config{
			Host: "http://localhost:8001", // from running `kubectl proxy`
		}
	} else if err != nil {
		return nil, fmt.Errorf("in cluster config: %s", err)
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("new for config: %s", err)
	}

	mypod, err := clientset.CoreV1().Pods(namespace).Get(ctx, pod, metav1.GetOptions{})
	if errors.IsNotFound(err) {
		return
	} else if statusError, isStatus := err.(*errors.StatusError); isStatus {
		return nil, fmt.Errorf("cannot get pod: %s", statusError.ErrStatus.Message)
	} else if err != nil {
		return
	}

	var claimName string
	for _, vol := range mypod.Spec.Volumes {
		if vol.PersistentVolumeClaim != nil {
			// Eventually, select a specific one in the list.. or match something
			if strings.HasPrefix(vol.PersistentVolumeClaim.ClaimName, prefix) {
				claimName = vol.PersistentVolumeClaim.ClaimName
			}
		}
	}
	if claimName == "" {
		return nil, fmt.Errorf("did not find any pvc")
	}

	mypvc, err := clientset.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, claimName, metav1.GetOptions{})
	if err != nil {
		return
	}
	pvName := mypvc.Spec.VolumeName

	mypv, err := clientset.CoreV1().PersistentVolumes().Get(ctx, pvName, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("getting pv %q: %s", pvName, err)
	}

	if mypv.Spec.GCEPersistentDisk == nil && mypv.Spec.CSI == nil {
		return nil, fmt.Errorf("no gce persistent disk")
	}

	labels := mypv.GetLabels()
	zone, ok := labels["failure-domain.beta.kubernetes.io/zone"]
	if !ok {
		zone, ok = labels["topology.kubernetes.io/zone"]
	}
	if !ok {
		return nil, fmt.Errorf("cannot find zone for PV %s, no failure-domain.beta.kubernetes.io/zone or topology.kubernetes.io/zone label on PV", pvName)
	}

	region, ok := labels["failure-domain.beta.kubernetes.io/region"]
	if !ok {
		region, ok = labels["topology.kubernetes.io/region"]
	}

	if !ok {
		return nil, fmt.Errorf("cannot find region for PV %s, no failure-domain.beta.kubernetes.io/region or topology.kubernetes.io/region label on PV", pvName)
	}

	return &pdDef{name: mypv.Spec.GCEPersistentDisk.PDName, zone: zone, region: region}, nil
}
