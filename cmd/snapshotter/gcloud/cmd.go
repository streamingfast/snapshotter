package gcloud

import (
	"bytes"
	"encoding/json"
	"fmt"
	"go.uber.org/zap"
	"io"
	"io/ioutil"
	"os/exec"
)

func GetSnapshots(project string) ([]Snapshot, error) {
	cmd := exec.Command("gcloud",
		"--project", project,
		"compute",
		"snapshots",
		"list",
		"--format", "json")
	zlog.Info("get snapshots", zap.Stringer("command", cmd))

	stdoout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	defer stdoout.Close()

	err = cmd.Start()
	if err != nil {
		return nil, err
	}

	data, err := ioutil.ReadAll(stdoout)
	if err != nil {
		return nil, err
	}

	var output []Snapshot
	if err := json.NewDecoder(bytes.NewReader(data)).Decode(&output); err != nil {
		if err != io.EOF {
			return nil, fmt.Errorf("could not read data %s", string(data))
		}
	}

	err = cmd.Wait()
	if err != nil {
		return nil, fmt.Errorf("make sure you are logged in: %s: %w", string(data), err)
	}

	return output, nil
}

func DeleteDisk(project, zone, diskName string) error {
	cmd := exec.Command("gcloud",
		"--project", project,
		"compute",
		"disks",
		"delete",
		diskName,
		"--zone", zone)
	zlog.Info("delete disk", zap.Stringer("command", cmd))

	err := cmd.Start()
	if err != nil {
		return err
	}

	err = cmd.Wait()
	if err != nil {
		return fmt.Errorf("make sure you are logged in: %w", err)
	}

	return nil
}

func CreateDiskFromSnapshot(project, zone, diskName, disksize, snapshotName string) error {
	cmd := exec.Command("gcloud",
		"--project", project,
		"compute",
		"disks",
		"create",
		"--size", disksize,
		"--source-snapshot", snapshotName,
		diskName,
		"--zone", zone,
		"--type", "pd-ssd")
	zlog.Info("create disk from snapshot", zap.Stringer("command", cmd))

	err := cmd.Start()
	if err != nil {
		return err
	}

	err = cmd.Wait()
	if err != nil {
		return fmt.Errorf("make sure you are logged in: %w", err)
	}

	return nil
}
