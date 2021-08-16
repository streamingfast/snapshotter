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
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"go.uber.org/atomic"
	"go.uber.org/zap"
	"google.golang.org/api/compute/v1"
	"google.golang.org/api/googleapi"
	batchv1 "k8s.io/api/batch/v1"
	v1 "k8s.io/api/core/v1"
	k8sErrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	watch "k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// A constant prefix used to denote that we not want to start from a snapshot but had an empty disk instead
const emptyDirSnapshotSuffix = "empty-disk"

type operator struct {
	k8sClient *kubernetes.Clientset
	gcpClient *compute.Service

	gcpProject     string
	zoneRoundRobin atomic.Uint64

	snapshotsLocks sync.Map
}

func newOperator(gcpProject string) (*operator, error) {
	config, err := rest.InClusterConfig()
	if err == rest.ErrNotInCluster {
		config = &rest.Config{
			Host: "http://localhost:8001", // from running `kubectl proxy`
		}
	} else if err != nil {
		return nil, fmt.Errorf("k8s InClusterConfig: %s", err)
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("k8s NewForConfig: %s", err)
	}

	ctx := context.Background()
	service, err := compute.NewService(ctx)
	if err != nil {
		return nil, fmt.Errorf("compute NewService: %s", err)
	}

	return &operator{
		k8sClient:  clientset,
		gcpClient:  service,
		gcpProject: gcpProject,
	}, nil
}

func (op *operator) Run(ctx context.Context) error {
	for {
		if err := op.oneRun(ctx); err != nil {
			zlog.Error("failed doing one run, waiting and restarting", zap.Error(err))
		}

		time.Sleep(10 * time.Second)
	}
}

func (op *operator) oneRun(ctx context.Context) error {
	listOpts := metav1.ListOptions{
		LabelSelector: "dfuse.io/snapshot-operator=true",
	}
	lst, err := op.k8sClient.BatchV1().Jobs("").Watch(ctx, listOpts)
	if err != nil {
		return fmt.Errorf("watching jobs: %s", err)
	}

	zlog.Info("listening on result channel")
	for ev := range lst.ResultChan() {
		if ev.Type == watch.Error {
			return fmt.Errorf("failed watching: %s", ev.Object)
		}

		job := ev.Object.(*batchv1.Job)
		switch ev.Type {
		case watch.Added, watch.Modified:
			zlog.Info("job changed", zap.String("job", job.Name), zap.String("type", string(ev.Type)))

			if job.Status.Active == 0 && job.Status.Failed == 0 && job.Status.Succeeded != 0 {
				if err := op.doDelete(ctx, job); err != nil {
					zlog.Info("deleted following job success", zap.String("job", job.Name), zap.Error(err))
				}
			} else {
				if err := op.doCreate(ctx, job); err != nil {
					zlog.Info("created because of new job", zap.String("job", job.Name), zap.Error(err))
				}
			}
		case watch.Deleted:
			if err := op.doDelete(ctx, job); err != nil {
				zlog.Info("job deleted, deleted PVC", zap.String("job", job.Name), zap.Error(err))
			}
		case watch.Bookmark:
			zlog.Info("received Bookmark event for job name", zap.String("job", job.Name))
		}
	}

	// Clean-up of left-over PVCs?

	// * Watch the Job definitions
	// * Watch the correspondig Pods (or watch the Pods that are jobs directly)
	// * If a given annotation is there `dfuse.io/require-snapshot`
	// * Create a snapshot
	// * Delete the pod in case it was waiting for too long
	return nil
}

func (op *operator) doDelete(ctx context.Context, job *batchv1.Job) error {
	snapshotName, _, err := op.getAnnotations(job)
	if err != nil {
		return nil
	}

	pvcName, _ := diskNamesFromJob(job, snapshotName)

	if op.isProcessing(snapshotName) {
		go func() {
			for {
				if !op.isProcessing(snapshotName) {
					op.deletePVC(ctx, job.Namespace, pvcName)
				}
				time.Sleep(3 * time.Second)
			}
		}()
	}

	op.deletePVC(ctx, job.Namespace, pvcName)

	return nil
}

func (op *operator) doCreate(ctx context.Context, job *batchv1.Job) error {
	snapshotName, pdZone, err := op.getAnnotations(job)
	if err != nil {
		return nil
	}

	zlog.Info("creating disk and pvc/pv for job processing", zap.String("snapshot", snapshotName), zap.String("zone", pdZone))

	if op.isProcessing(snapshotName) {
		zlog.Info("already processing", zap.String("snapshot", snapshotName))
		return nil
	}

	pvcName, pdName := diskNamesFromJob(job, snapshotName)

	diskExists := true
	// Check target disk
	_, err = op.gcpClient.Disks.Get(op.gcpProject, pdZone, pdName).Do()
	if err != nil {
		if isNotFound(err) {
			diskExists = false
		} else {
			zlog.Info("failed querying disk from gcp", zap.String("pd_name", pdName), zap.Error(err))
			return nil
		}
	}

	op.markProcessing(snapshotName)

	go op.createDisk(ctx, job, diskExists, snapshotName, pdZone, pvcName, pdName)

	return nil
}

func (op *operator) deletePVC(ctx context.Context, namespace, pvcName string) {
	zlog.Info("deleting pvc", zap.String("pvc_name", pvcName))

	err := op.k8sClient.CoreV1().PersistentVolumeClaims(namespace).Delete(ctx, pvcName, metav1.DeleteOptions{})
	if err == nil {
		// Completed successfully
		return
	}

	if k8sErrors.IsNotFound(err) {
		zlog.Info("pvc was already deleted, assuming successful completion")
		return
	}

	if err != nil {
		zlog.Error("couldn't delete PVC", zap.Error(err), zap.String("pvc_name", pvcName), zap.String("namespace", namespace))
	}
}

func (op *operator) markProcessing(snapshot string) {
	op.snapshotsLocks.Store(snapshot, true)
}
func (op *operator) markDone(snapshot string) {
	op.snapshotsLocks.Delete(snapshot)
}
func (op *operator) isProcessing(snapshot string) bool {
	_, ok := op.snapshotsLocks.Load(snapshot)
	return ok
}

func (op *operator) getAnnotations(job *batchv1.Job) (snapshotName, pdZone string, err error) {
	snapshotName = job.Annotations["dfuse.io/snapshot-source"]
	if snapshotName == "" {
		zlog.Info("missing `dfuse.io/snapshot-source` annotation on job", zap.String("job", job.Name))
		return "", "", errors.New("failed")
	}

	pdZone = job.Annotations["dfuse.io/snapshot-to-zone"]
	if pdZone == "" {
		zlog.Info("missing `dfuse.io/snapshot-to-zone` annotation on job", zap.String("job", job.Name))
		return "", "", errors.New("failed")
	}

	return
}

func shouldUseSnapshotForDisk(snapshotName string) bool {
	return !strings.HasSuffix(snapshotName, emptyDirSnapshotSuffix)
}

func (op *operator) createDisk(ctx context.Context, job *batchv1.Job, diskExists bool, snapshotName, pdZone, pvcName, pdName string) {
	defer op.markDone(snapshotName)

	if !diskExists {
		sourceSnapshot := ""
		sizeInGb := int64(0)
		if shouldUseSnapshotForDisk(snapshotName) {
			snapshot, err := op.gcpClient.Snapshots.Get(op.gcpProject, snapshotName).Do()
			if err != nil {
				zlog.Error("failed getting required snapshot", zap.Error(err))
				return
			}
			sizeInGb = snapshot.DiskSizeGb + 50 // TODO: this is hardcoded, deal with it

			sourceSnapshot = snapshot.SelfLink
		} else {
			// TODO: This is hard-coded, but the empty dir size could be retrieved from an annotation on resource
			sizeInGb = 50
		}

		gcpDisk := &compute.Disk{
			Description:    "created by snapshotter, from " + snapshotName,
			Name:           pdName,
			SourceSnapshot: sourceSnapshot,
			SizeGb:         sizeInGb,
			Type:           "projects/" + op.gcpProject + "/zones/" + pdZone + "/diskTypes/pd-ssd",
		}

		// Fetch the snapshot source
		_, err := op.gcpClient.Disks.Insert(op.gcpProject, pdZone, gcpDisk).Do()
		if err != nil {
			zlog.Error("failed creating volume from snapshot", zap.Error(err))
			return
		}
	}

	targetSize := "1Gi"
	// Loop until the created disk is ready
	zlog.Info("polling persistent disk until ready", zap.String("pd_name", pdName))
	for {
		disk, err := op.gcpClient.Disks.Get(op.gcpProject, pdZone, pdName).Do()
		if isNotFound(err) {
			zlog.Error("expecting pd to exist when we JUST created it", zap.String("pd_name", pdName))
			return
		}
		if err != nil {
			zlog.Error("error polling pd before creating pvc/pv", zap.String("pd_name", pdName), zap.Error(err))
			return
		}

		if disk.Status == "READY" {
			zlog.Info("persistent disk ready, creating pvc/pv", zap.String("pd_name", pdName))
			targetSize = fmt.Sprintf("%dGi", disk.SizeGb)
			break
		}

		time.Sleep(10 * time.Second)
	}

	_, err := op.k8sClient.CoreV1().PersistentVolumeClaims(job.Namespace).Create(ctx, &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: job.Namespace,
			Name:      pvcName,
		},
		Spec: v1.PersistentVolumeClaimSpec{
			Resources: v1.ResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceName("storage"): resource.MustParse(targetSize),
				},
			},
			StorageClassName: S(""),
			VolumeName:       pvcName,
			AccessModes:      []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce},
		},
	}, metav1.CreateOptions{})

	if err != nil {
		zlog.Error("creating PVC", zap.Error(err))
		return
	}
	zlog.Info("pvc created", zap.String("pvc_name", pvcName))

	pvName := pvcName
	_, err = op.k8sClient.CoreV1().PersistentVolumes().Create(ctx, &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: pvName,
		},
		Spec: v1.PersistentVolumeSpec{
			Capacity: v1.ResourceList{
				v1.ResourceName("storage"): resource.MustParse(targetSize),
			},
			AccessModes:                   []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce},
			PersistentVolumeReclaimPolicy: v1.PersistentVolumeReclaimDelete,
			PersistentVolumeSource: v1.PersistentVolumeSource{
				GCEPersistentDisk: &v1.GCEPersistentDiskVolumeSource{
					PDName: pdName,
					FSType: "ext4",
				},
			},
		},
	}, metav1.CreateOptions{})
	if err != nil {
		zlog.Error("creating PV", zap.String("pv_name", pvName), zap.Error(err))
		return
	}
	zlog.Info("pv created", zap.String("pv_name", pvName))
}

func diskNamesFromJob(job *batchv1.Job, snapshotName string) (pvcName, pdName string) {
	for _, vol := range job.Spec.Template.Spec.Volumes {
		if vol.VolumeSource.PersistentVolumeClaim != nil {
			jobClaimName := vol.VolumeSource.PersistentVolumeClaim.ClaimName
			if strings.Contains(jobClaimName, snapshotName) {
				pvcName = jobClaimName
			}
		}
	}

	return pvcName, pvcName
}

func isNotFound(err error) bool {
	if gapiErr, ok := err.(*googleapi.Error); ok {
		return gapiErr.Code == 404
	}
	return false
}

func S(in string) *string {
	return &in
}
