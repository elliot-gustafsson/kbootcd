package host

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/elliot-gustafsson/kbootcd/internal/command"
	"github.com/google/go-containerregistry/pkg/name"
)

// BootcHost represents the full JSON payload returned by `bootc status --json`
type BootcHost struct {
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`
	// Metadata   struct {
	// 	Name string `json:"name"`
	// } `json:"metadata"`
	// Spec   BootcSpec   `json:"spec"`
	Status BootcStatus `json:"status"`
}

// type BootcSpec struct {
// 	BootOrder string         `json:"bootOrder"`
// 	Image     ContainerImage `json:"image"`
// }

type BootcStatus struct {
	Booted   BootcState  `json:"booted"`
	Staged   *BootcState `json:"staged,omitempty"`
	Rollback *BootcState `json:"rollback,omitempty"`
}

// BootcState represents a single boot environment state (e.g. what is booted, staged, etc)
type BootcState struct {
	Image StateImage `json:"image"`
}
type StateImage struct {
	Image       ContainerImage `json:"image"`
	ImageDigest string         `json:"imageDigest"`
	Version     string         `json:"version"`
	Timestamp   time.Time      `json:"timestamp"`
}

// ContainerImage is reused in both Spec and Status
type ContainerImage struct {
	Image     string `json:"image"`
	Transport string `json:"transport"`
}

func (t *StateImage) GetDigest() (d name.Digest, err error) {
	d, err = name.NewDigest(fmt.Sprintf("%s@%s", t.Image.Image, t.ImageDigest))
	if err != nil {
		return d, fmt.Errorf("error parsing booted image digest, err: %w", err)
	}
	return
}

// getBootedImageDigest queries the physical node namespace to unmarshal system configurations
func GetBootcHostStatus(ctx context.Context, cmder command.Commander) (status BootcStatus, err error) {
	jsonOut, err := runOnHost(ctx, cmder, "bootc", "status", "--json")
	if err != nil {
		return status, fmt.Errorf("bootc status process failure: %w", err)
	}

	var hostStatus BootcHost

	if err := json.Unmarshal(jsonOut, &hostStatus); err != nil {
		return status, fmt.Errorf("malformed json interface return: %w", err)
	}
	status = hostStatus.Status
	return
}

func BootcSwitch(ctx context.Context, cmder command.Commander, image string) error {
	_, err := runOnHost(ctx, cmder, "bootc", "switch", image)
	return err
}
