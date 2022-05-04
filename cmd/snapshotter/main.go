package main

import (
	"github.com/spf13/pflag"
	. "github.com/streamingfast/cli"
	"github.com/streamingfast/logging"
)

var zlog, _ = logging.RootLogger("snapshotter", "github.com/streamingfast/snapshotter/cmd/snapshotter")

func init() {
	logging.InstantiateLoggers()
}

func main() {
	Run("snapshotter", "Infra tools for StreamingFast environment",
		ConfigureViper("SNAPSHOTTER"),

		PersistentFlags(func(flags *pflag.FlagSet) {
			flags.StringP("project", "p", "", "gcloud project name")
		}),

		Command(restoreSnapshotE,
			"restore <namespace> <pod> <snapshot>",
			"Restore a disk to specific snapshot, use latest to restore from the latest snapshot",
			Description(`
				Find the snapshot from within the GCP project (via flag '--project') passed
				via the <snapshot> argument. If the received argument is named latest, in this
				case we find the most recent snapshot for the given <namespace> (the snapshot's
				name must start with the namespace for this to work properly.).

				It then deletes existing pod and its disk, create a new disk from the snapshot
				given and then start back the pod with it attaching it the newly created disk.

				The disk creation happens through GCP APIs and is then attached to the pod via
				the PVC using Kubernetes APIs.

				You can find latest snapshots with

					gcloud compute snapshots list | grep eth-mainnet | tail -n 5

				That will give you the last 5 snapshots that matches 'eth-mainnet'.

				> Ensure that your 'gcloud' instance is configured with the right GCP project
				> Don't forget to update 'eth-mainnet' for what you need!

				**Note** You can define SNAPSHOTTER_GLOBAL_PROJECT to avoid passing --project each time
			`),
			ExamplePrefixed("snapshotter", `
				restore eth-mainnet mindreader-v3-1 latest
				restore eth-mainnet mindreader-v3-1 eth-mainnet-v2-0013642743
			`),
			ExactArgs(3),
		),
	)
}
