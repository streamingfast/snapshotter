package gcloud

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

type Snapshot struct {
	Created time.Time `json:"creationTimestamp"`
	Name    string    `json:"name"`
	Size    string    `json:"diskSizeGb"`
}

func (snap *Snapshot) GetSize() string {
	return fmt.Sprintf("%sG", snap.Size)
}

func (snap *Snapshot) GetName() string {
	return snap.Name
}

func (snap *Snapshot) Matches(namespace string) bool {
	if !strings.HasPrefix(snap.Name, namespace) {
		return false
	}

	return true
}

func FindSnapshot(snapshots []Snapshot, snapshotName, namespace string) (*Snapshot, error) {
	if len(snapshots) == 0 {
		return nil, fmt.Errorf("no snapshots received, unable to find anything in this")
	}

	if snapshotName != "latest" {
		for _, snap := range snapshots {
			if snap.Name == snapshotName {
				return &snap, nil
			}
		}

		return nil, fmt.Errorf("cannot find snapshot named %q among %d snapshots", snapshotName, len(snapshots))
	}

	found := make([]Snapshot, 0, len(snapshots))
	for _, snap := range snapshots {
		if snap.Matches(namespace) {
			found = append(found, snap)
		}
	}

	if len(found) == 0 {
		return nil, fmt.Errorf("cannot find snapshot prefixed with namespace %q among %d snapshots", namespace, len(snapshots))
	}

	// Reverse sort
	sort.Slice(found, func(i, j int) bool {
		return found[i].Created.After(found[j].Created)
	})

	return &found[0], nil
}
