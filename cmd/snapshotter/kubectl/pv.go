package kubectl

import (
	"fmt"
	"strings"
)

type PVOutput struct {
	Items []PersistentVolume `json:"items"`
}

type PersistentVolume struct {
	Metadata Metadata `json:"metadata"`
	Spec     Spec     `json:"spec"`
}

type Metadata struct {
	Name   string                 `json:"name"`
	Labels map[string]interface{} `json:"labels"`
}

type Spec struct {
	ClaimRef ClaimRef          `json:"claimRef,omitempty"`
	Csi      Csi               `json:"csi"`
	Disk     GCEPersistentDisk `json:"gcePersistentDisk,omitempty"`
}

type Csi struct {
	VolumeHandle string `json:"volumeHandle"`
}
type GCEPersistentDisk struct {
	PDName string `json:"pdName"`
}

type ClaimRef struct {
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
}

func (pv *PersistentVolume) MatchesApp(namespace string, appName string, mountName *string) bool {
	claim := pv.Spec.ClaimRef.Name

	if pv.Spec.ClaimRef.Namespace != namespace {
		return false
	}

	if !strings.HasSuffix(claim, "-"+appName) {
		return false
	}

	if mountName == nil {
		return true
	}

	if !strings.HasPrefix(claim, *mountName) {
		return false
	}

	return true
}

func (pv *PersistentVolume) GetName() (string, error) {
	return pv.Metadata.Name, nil
}

func (pv *PersistentVolume) GetZone() (string, error) {
	zone, ok := pv.Metadata.Labels["failure-domain.beta.kubernetes.io/zone"]
	if !ok {
		zone, ok = pv.Metadata.Labels["topology.kubernetes.io/zone"]
	}
	if !ok {
		if pv.Spec.Csi.VolumeHandle != "" { // PVs that support VolumeSnapshots use new spec model
			fields := strings.Split(pv.Spec.Csi.VolumeHandle, "/")
			prevField := ""
			for _, f := range fields {
				if prevField == "zones" {
					return f, nil
				}
				prevField = f
			}
		}

		return "", fmt.Errorf("zone not found")
	}
	return zone.(string), nil
}

func (pv *PersistentVolume) GetGCEDisk() (string, error) {
	if pv.Spec.Disk.PDName != "" {
		return pv.Spec.Disk.PDName, nil
	}
	if pv.Spec.Csi.VolumeHandle != "" { // PVs that support VolumeSnapshots use new spec model
		fields := strings.Split(pv.Spec.Csi.VolumeHandle, "/")
		prevField := ""
		for _, f := range fields {
			if prevField == "disks" {
				return f, nil
			}
			prevField = f
		}
	}

	return "", fmt.Errorf("cannot find GCE disk name")

}

func Find(pvs []PersistentVolume, namespace string, appName string, mountName *string) (*PersistentVolume, error) {
	for _, pv := range pvs {
		if pv.MatchesApp(namespace, appName, mountName) {
			return &pv, nil
		}
	}

	return nil, fmt.Errorf("not found")
}
