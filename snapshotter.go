package snapshotter

import (
	"context"
	"fmt"
	"os"
	"time"
)

type GKEPVCSnapshotter struct {
	tag       string
	project   string
	namespace string
	pod       string
	prefix    string
	archive   bool
}

var gkeExampleConfigString = "type=gke-pvc-snapshot tag=v1 namespace=default project=mygcpproject prefix=datadir archive=true"

func NewGKEPVCSnapshotter(conf map[string]string) (*GKEPVCSnapshotter, error) {
	for _, label := range []string{"tag", "project", "namespace", "prefix", "archive"} {
		if err := gkeCheckMissing(conf, label); err != nil {
			return nil, err
		}
	}
	return &GKEPVCSnapshotter{
		tag:       conf["tag"],
		project:   conf["project"],
		namespace: conf["namespace"],
		pod:       os.Getenv("HOSTNAME"),
		prefix:    conf["prefix"],
		archive:   conf["archive"] == "true",
	}, nil
}

func (s *GKEPVCSnapshotter) RequiresStop() bool {
	return true
}

func (s *GKEPVCSnapshotter) Backup(lastSeenBlockNum uint32) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	snapshotName := GenerateName(s.namespace, s.tag, lastSeenBlockNum)
	return snapshotName, TakeSnapshot(ctx, snapshotName, s.project, s.namespace, s.pod, s.prefix, s.archive)

}

func gkeCheckMissing(conf map[string]string, param string) error {
	if conf[param] == "" {
		return fmt.Errorf("backup module gke-pvc-snapshot missing value for %s. Example: %s", param, gkeExampleConfigString)
	}
	return nil
}
