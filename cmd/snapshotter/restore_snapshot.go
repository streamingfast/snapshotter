package main

import (
	"fmt"
	"time"

	"github.com/streamingfast/snapshotter/cmd/snapshotter/gcloud"
	"github.com/streamingfast/snapshotter/cmd/snapshotter/kubectl"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

func restoreSnapshotE(cmd *cobra.Command, args []string) error {
	project := viper.GetString("global-project")
	if project == "" {
		return fmt.Errorf("--project (-p) flag must be defined")
	}

	namespace := args[0]
	podName := args[1]
	snapshotName := args[2]

	sts, err := kubectl.GetStatefulSetFromPod(podName)
	if err != nil {
		return fmt.Errorf("could not get stateful set from pod: %w", err)
	}

	stsDefinitionFile, cleanupFunc, err := kubectl.GetStatefulSetDefinitionFile(sts, namespace)
	if err != nil {
		return fmt.Errorf("could not get statefulset definition: %w", err)
	}
	zlog.Info("statefulset definition file created", zap.String("file", stsDefinitionFile))

	snaps, err := gcloud.GetSnapshots(project)
	if err != nil {
		return fmt.Errorf("could not get snapshots list: %w", err)
	}

	snap, err := gcloud.FindSnapshot(snaps, snapshotName, namespace)
	if err != nil {
		return fmt.Errorf("could not get latest snapshot for namespace: %w", err)
	}
	zlog.Info("selected a snapshot that will be restored", zap.String("snapshot", snap.Name))

	pvs, err := kubectl.GetPVs()
	if err != nil {
		return fmt.Errorf("could not list pvs: %w", err)
	}

	pv, err := kubectl.Find(pvs, namespace, podName, nil)
	if err != nil {
		return fmt.Errorf("could not find pv for pod: %w", err)
	}

	zone, err := pv.GetZone()
	if err != nil {
		return err
	}
	disk, err := pv.GetGCEDisk()
	if err != nil {
		return err
	}

	zlog.Info("deleting statefulset definition", zap.String("statefulset", sts), zap.String("namespace", namespace))
	err = kubectl.DeleteStatefulSet(sts, namespace)
	if err != nil {
		return fmt.Errorf("could not delete statefulset: %w", err)
	}

	zlog.Info("deleting pod", zap.String("pod", podName), zap.String("namespace", namespace))
	err = kubectl.DeletePod(podName, namespace)
	if err != nil {
		return fmt.Errorf("could not delete pod: %w", err)
	}

	for i := 0; true; i++ { // retries
		zlog.Info(
			"deleting old disk",
			zap.String("disk", disk),
			zap.String("zone", zone),
			zap.String("project", project),
		)
		err = gcloud.DeleteDisk(project, zone, disk)
		if err != nil {
			if i > 20 {
				return fmt.Errorf("could not delete disk %s in zone %s: %w", disk, zone, err)
			}

			time.Sleep(time.Second * 5)
			zlog.Info("retrying disk deletion", zap.Error(err))
			continue
		}
		break
	}

	zlog.Info(
		"creating new disk from snapshot",
		zap.String("disk", disk),
		zap.String("size", snap.GetSize()),
		zap.String("snapshot", snap.GetName()),
	)
	err = gcloud.CreateDiskFromSnapshot(project, zone, disk, snap.GetSize(), snap.GetName())
	if err != nil {
		return fmt.Errorf("could not create disk %s in zone %s from snapshot %s: %w", disk, zone, snap.GetName(), err)
	}

	zlog.Info(
		"recreating statefulset from definition",
		zap.String("statefulset", sts),
		zap.String("namespace", namespace),
	)
	err = kubectl.CreateStatefulSetFromFile(stsDefinitionFile)
	if err != nil {
		return fmt.Errorf("could not create statefulset: %w", err)
	}

	cleanupFunc()
	return nil
}
