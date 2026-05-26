package controller

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/elliot-gustafsson/kbootcd/internal/command"
	"github.com/elliot-gustafsson/kbootcd/internal/host"
	"github.com/google/go-containerregistry/pkg/name"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/kubectl/pkg/drain"
	"k8s.io/utils/clock"

	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type RequiredAction uint8

const (
	ActionNone RequiredAction = iota
	ActionReboot
	ActionStageAndReboot
)

func (a RequiredAction) None() bool {
	return a == ActionNone
}

func (a RequiredAction) Stage() bool {
	return a == ActionStageAndReboot
}

func (a RequiredAction) Reboot() bool {
	return a == ActionReboot || a == ActionStageAndReboot
}

type Config struct {
	NodeName              string
	TargetImageConfigPath string
	LeaseName             string
	LeaseNamespace        string
	LeaseDuration         time.Duration
	Clock                 clock.PassiveClock
	Window                Window
}

func Reconcile(ctx context.Context, cmder command.Commander, kube kubernetes.Interface, config Config) error {
	desiredBytes, err := os.ReadFile(config.TargetImageConfigPath)
	if err != nil {
		return fmt.Errorf("error reading target config file at %s, err: %w", config.TargetImageConfigPath, err)
	}
	desiredImage := strings.TrimSpace(string(desiredBytes))

	status, err := host.GetBootcHostStatus(ctx, cmder)
	if err != nil {
		return fmt.Errorf("error getting bootc status, err: %w", err)
	}

	logger := slog.With(
		"node", config.NodeName,
		"booted_image", fmt.Sprintf("%s@%s", status.Booted.Image.Image.Image, status.Booted.Image.ImageDigest),
		"desired_image", desiredImage,
	)

	requiredAction, err := getRequiredAction(&status, desiredImage)
	if err != nil {
		return err
	}

	reboot := requiredAction.Reboot()

	if requiredAction.Stage() {
		logger.Info("staging base OS container update layers...")
		if err := host.BootcSwitch(ctx, cmder, desiredImage); err != nil {
			return fmt.Errorf("error executing bootc switch, err: %w", err)
		}

		// refetch status to get up to date status
		postStageStatus, err := host.GetBootcHostStatus(ctx, cmder)
		if err != nil {
			return fmt.Errorf("error getting bootc status, err: %w", err)
		}

		if reboot {

			switch {
			case postStageStatus.Staged == nil:
				// If noting is staged, skip reboot
				reboot = false
			case postStageStatus.Booted.Image.ImageDigest == postStageStatus.Staged.Image.ImageDigest:
				// If staged image is same as booted, skip reboot
				reboot = false
			}

			if !reboot {
				logger.Info("bootc switch completed but no new update was staged. Node is already up to date")
			}

		}
	}

	if !reboot {
		err := ensureUncordoned(ctx, logger, kube, config)
		if err != nil {
			logger.Warn("failed uncordoning node", "error", err)
		}
		err = ensureLeaseReleased(ctx, logger, kube, config)
		if err != nil {
			logger.Warn("failed releasing lease lock", "error", err)
		}
		return nil
	}

	// evaluate reboot window
	if !config.Window.Contains(config.Clock.Now()) {
		logger.Info("staging successful. outside allowed reboot window boundaries, pausing sequence.")
		return nil
	}

	lease, err := tryAcquireLease(ctx, kube, config)
	if err != nil {
		return fmt.Errorf("error acquiring api lease, err: %w", err)
	}
	if lease == nil {
		logger.Info("lock active on peer node, queueing drain execution...")
		return nil
	}

	logger.Info("lease lock acquired")

	healthy, err := isClusterHealthy(ctx, kube, config.NodeName)
	if err != nil {
		logger.Error("failed to verify cluster health", "error", err)
		return err
	}

	if !healthy {
		logger.Warn("cluster is degraded. holding lock and pausing upgrade sequence until cluster is healthy.")
		return nil
	}
	logger.Info("cluster health verified. starting drain...")

	if err := drainNode(ctx, logger, kube, lease, config); err != nil {
		return fmt.Errorf("error draining node, err: %w", err)
	}

	logger.Info("workload eviction finalized, issuing reboot...")
	return host.Reboot(ctx, cmder)
}

func getRequiredAction(status *host.BootcStatus, desired string) (RequiredAction, error) {
	desiredReference, err := name.ParseReference(desired)
	if err != nil {
		return ActionNone, fmt.Errorf("error parsing desired image reference, err: %w", err)
	}

	bootedDigest, err := status.Booted.Image.GetDigest()
	if err != nil {
		return ActionNone, fmt.Errorf("error parsing booted image digest, err: %w", err)
	}

	if d, ok := desiredReference.(name.Digest); ok {
		if d.DigestStr() == bootedDigest.DigestStr() {
			return ActionNone, nil
		}

		if status.Staged != nil {
			stagedDigest, err := status.Staged.Image.GetDigest()
			if err != nil {
				// Not fatal, just log and continue upgrade process
				slog.Warn("error parsing staged image digest", "error", err.Error())
				return ActionStageAndReboot, nil
			}

			if d.DigestStr() == stagedDigest.DigestStr() {
				// If the staged digest is the desired one, just reboot
				return ActionReboot, nil
			}
		}
	}

	// If the desired image isnt a digest we cannot be sure that we are up to date
	return ActionStageAndReboot, nil
}

func isClusterHealthy(ctx context.Context, kube kubernetes.Interface, ourNodeName string) (bool, error) {
	nodes, err := kube.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return false, fmt.Errorf("failed to list nodes to check health: %w", err)
	}
	for _, node := range nodes.Items {
		// we don't care about our own node's status
		if node.Name == ourNodeName {
			continue
		}

		isReady := false
		for _, condition := range node.Status.Conditions {
			if condition.Type == corev1.NodeReady {
				if condition.Status == corev1.ConditionTrue {
					isReady = true
				}
				break
			}
		}
		if !isReady {
			// cluster is degraded.
			return false, nil
		}
	}
	return true, nil
}

// drainNode builds safe declarative eviction maps targeting unmanaged local infrastructure
func drainNode(ctx context.Context, logger *slog.Logger, kube kubernetes.Interface, lease *coordinationv1.Lease, config Config) error {

	drainCtx, drainCtxCancel := context.WithCancel(ctx)
	defer drainCtxCancel()

	go func() {
		leases := kube.CoordinationV1().Leases(config.LeaseNamespace)
		t := time.Tick(60 * time.Second)
		for {
			select {
			case <-drainCtx.Done():
				return
			case <-t:
				lease.Spec.HolderIdentity = &config.NodeName
				lease.Spec.RenewTime = &metav1.MicroTime{Time: config.Clock.Now()}
				_, err := leases.Update(ctx, lease, metav1.UpdateOptions{})
				if err != nil {
					logger.Error("error refreshing lease", "error", err.Error())
				}
			}
		}
	}()

	node, err := kube.CoreV1().Nodes().Get(ctx, config.NodeName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("failed locating local node entity object: %w", err)
	}

	helper := &drain.Helper{
		Ctx:                 ctx,
		Client:              kube,
		Force:               true, // Ignores orphaned unmanaged pods
		GracePeriodSeconds:  -1,   // Honors user-configured workload termination grace limits
		IgnoreAllDaemonSets: true, // Prevents eviction loop lockups targeting itself
		DeleteEmptyDirData:  true, // Drops ephemeral cache disks
		Out:                 os.Stdout,
		ErrOut:              os.Stderr,
	}

	logger.Info("draining node...")

	if err := drain.RunCordonOrUncordon(helper, node, true); err != nil {
		return fmt.Errorf("error cordoning node, err: %w", err)
	}

	if err := drain.RunNodeDrain(helper, config.NodeName); err != nil {
		return fmt.Errorf("error draining node, err: %w", err)
	}

	return nil
}

// ensureUncordoned updates base status payloads to resume scheduler tracking
func ensureUncordoned(ctx context.Context, logger *slog.Logger, kube kubernetes.Interface, config Config) error {
	node, err := kube.CoreV1().Nodes().Get(ctx, config.NodeName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if !node.Spec.Unschedulable {
		return nil
	}

	logger.Info("target state validated. uncordoning node scheduling attributes...")
	patch := []byte(`{"spec":{"unschedulable":false}}`)
	_, err = kube.CoreV1().Nodes().Patch(ctx, config.NodeName, types.StrategicMergePatchType, patch, metav1.PatchOptions{})
	return err
}

// tryAcquireLease evaluates distributed locks across cluster boundaries
func tryAcquireLease(ctx context.Context, kube kubernetes.Interface, config Config) (*coordinationv1.Lease, error) {
	leases := kube.CoordinationV1().Leases(config.LeaseNamespace)

	lease, err := leases.Get(ctx, config.LeaseName, metav1.GetOptions{})
	if k8serrors.IsNotFound(err) {
		// Bootstrap initialization if Lease payload is missing inside state layer
		leaseDuration := int32(config.LeaseDuration.Seconds())
		newLease := &coordinationv1.Lease{
			ObjectMeta: metav1.ObjectMeta{Name: config.LeaseName, Namespace: config.LeaseNamespace},
			Spec: coordinationv1.LeaseSpec{
				HolderIdentity:       &config.NodeName,
				AcquireTime:          &metav1.MicroTime{Time: config.Clock.Now()},
				RenewTime:            &metav1.MicroTime{Time: config.Clock.Now()},
				LeaseDurationSeconds: &leaseDuration,
			},
		}
		_, err := leases.Create(ctx, newLease, metav1.CreateOptions{})

		if k8serrors.IsAlreadyExists(err) {
			// we lost the race
			return nil, nil
		} else if err != nil {
			return nil, err
		}

		return newLease, err
	} else if err != nil {
		return nil, err
	}

	isExpired := false
	if lease.Spec.RenewTime != nil && lease.Spec.LeaseDurationSeconds != nil {
		expiryTime := lease.Spec.RenewTime.Time.Add(time.Duration(*lease.Spec.LeaseDurationSeconds) * time.Second)
		isExpired = config.Clock.Now().After(expiryTime)
	}

	// Evaluate existing record authorization status
	if lease.Spec.HolderIdentity == nil || *lease.Spec.HolderIdentity == "" || *lease.Spec.HolderIdentity == config.NodeName || isExpired {
		leaseDuration := int32(config.LeaseDuration.Seconds())
		lease.Spec.HolderIdentity = &config.NodeName
		lease.Spec.AcquireTime = &metav1.MicroTime{Time: config.Clock.Now()}
		lease.Spec.RenewTime = &metav1.MicroTime{Time: config.Clock.Now()}
		lease.Spec.LeaseDurationSeconds = &leaseDuration

		_, err := leases.Update(ctx, lease, metav1.UpdateOptions{})
		if k8serrors.IsConflict(err) {
			// we lost the race
			return nil, nil
		} else if err != nil {
			return nil, err
		}

		return lease, err
	}

	return nil, nil
}

// ensureLeaseReleased clears explicit distributed resource lock definitions
func ensureLeaseReleased(ctx context.Context, logger *slog.Logger, kube kubernetes.Interface, config Config) error {
	leases := kube.CoordinationV1().Leases(config.LeaseNamespace)
	lease, err := leases.Get(ctx, config.LeaseName, metav1.GetOptions{})
	if k8serrors.IsNotFound(err) {
		return nil
	} else if err != nil {
		return err
	}

	if lease.Spec.HolderIdentity != nil && *lease.Spec.HolderIdentity == config.NodeName {
		logger.Info("releasing cluster upgrade lock object...")
		lease.Spec.HolderIdentity = nil
		_, err := leases.Update(ctx, lease, metav1.UpdateOptions{})
		return err
	}
	return nil
}
