package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/elliot-gustafsson/kbootcd/internal/command"
	"github.com/elliot-gustafsson/kbootcd/internal/host"
	"github.com/elliot-gustafsson/kbootcd/internal/slogwriter"
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

const (
	AnnotationLastUpToDate = "kbootcd/last-up-to-date"
	AnnotationStagedAt     = "kbootcd/staged-at"

	ControlPlaneRoleLabel = "node-role.kubernetes.io/control-plane"
	MasterRoleLabel       = "node-role.kubernetes.io/master"

	controlPlaneSyncGrace = 2 * time.Minute
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
	RebootDelay           time.Duration
	CooldownDelay         time.Duration
	Clock                 clock.PassiveClock
	Window                Window
}

// ErrRebootInitiated is returned when the reconciliation successfully triggers a host reboot.
var ErrRebootInitiated = errors.New("reboot initiated")

func Reconcile(ctx context.Context, cmder command.Commander, kube kubernetes.Interface, config Config) error {
	node, err := waitForNodeReady(ctx, kube, config.NodeName)
	if err != nil {
		return err
	}
	isControlPlane := isControlPlaneNode(node)

	desiredBytes, err := os.ReadFile(config.TargetImageConfigPath)
	if err != nil {
		return fmt.Errorf("error reading target config file at %s, err: %w", config.TargetImageConfigPath, err)
	}
	desiredImage := strings.TrimSpace(string(desiredBytes))

	status, err := host.GetBootcHostStatus(ctx, cmder)
	if err != nil {
		return fmt.Errorf("error getting bootc status, err: %w", err)
	}

	preStagedDigest := stagedDigest(&status)
	postStagedDigest := preStagedDigest

	bootedImage, err := status.Booted.Image.GetDigest()
	if err != nil {
		slog.Error("failed parsing booted image", "error", err.Error())
	}

	logger := slog.With(
		"node", config.NodeName,
		"booted_image", bootedImage,
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
		postStagedDigest = stagedDigest(&postStageStatus)

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
		lease, err := getLease(ctx, kube, config)
		if err != nil {
			return fmt.Errorf("failed getting lease, err: %w", err)
		}

		// Only run cleanup if we're leader
		if lease != nil && lease.Spec.HolderIdentity != nil && *lease.Spec.HolderIdentity == config.NodeName {

			err := ensureUncordoned(ctx, logger, kube, config)
			if err != nil {
				return fmt.Errorf("failed uncordoning node, err: %w", err)
			}

			err = finalizeLeaseState(ctx, logger, kube, lease, config)
			if err != nil {
				return fmt.Errorf("failed finalizing lease lock state, err: %w", err)
			}
		}

		if isControlPlane {
			nowStr := config.Clock.Now().UTC().Format(time.RFC3339)
			if err := setNodeAnnotation(ctx, kube, config.NodeName, AnnotationLastUpToDate, nowStr); err != nil {
				return fmt.Errorf("failed setting %s annotation, err: %w", AnnotationLastUpToDate, err)
			}
		} else if node.Annotations[AnnotationStagedAt] != "" {
			if err := removeNodeAnnotation(ctx, kube, config.NodeName, AnnotationStagedAt); err != nil {
				return fmt.Errorf("failed removing %s annotation, err: %w", AnnotationStagedAt, err)
			}
		}

		return nil
	}

	var workerStagedAt time.Time

	if isControlPlane {
		if node.Annotations[AnnotationLastUpToDate] != "" {
			if err := removeNodeAnnotation(ctx, kube, config.NodeName, AnnotationLastUpToDate); err != nil {
				return fmt.Errorf("failed removing %s annotation, err: %w", AnnotationLastUpToDate, err)
			}
		}
	} else {
		existingStagedAt := node.Annotations[AnnotationStagedAt]
		if existingStagedAt != "" {
			parsed, err := time.Parse(time.RFC3339, existingStagedAt)
			if err != nil {
				logger.Warn("invalid staged-at annotation on node, resetting", "value", existingStagedAt, "error", err)
				existingStagedAt = ""
			} else {
				workerStagedAt = parsed
			}
		}

		if preStagedDigest != postStagedDigest || existingStagedAt == "" {
			now := config.Clock.Now().UTC().Truncate(time.Second)
			nowStr := now.Format(time.RFC3339)
			if err := setNodeAnnotation(ctx, kube, config.NodeName, AnnotationStagedAt, nowStr); err != nil {
				return fmt.Errorf("failed setting %s annotation, err: %w", AnnotationStagedAt, err)
			}
			workerStagedAt = now
		}
	}

	// evaluate reboot window
	if !config.Window.Contains(config.Clock.Now()) {
		logger.Debug("staging successful. outside allowed reboot window boundaries, pausing sequence.")
		return nil
	}

	if !isControlPlane {
		done, err := areControlPlaneNodesDone(ctx, logger, kube, workerStagedAt)
		if err != nil {
			return fmt.Errorf("error checking control plane upgrade status, err: %w", err)
		}
		if !done {
			logger.Debug("waiting for control plane nodes to finish upgrading before acquiring lock...")
			return nil
		}
	}

	lease, err := tryAcquireLease(ctx, kube, config)
	if err != nil {
		return fmt.Errorf("error acquiring api lease, err: %w", err)
	}
	if lease == nil {
		logger.Debug("lock active on peer node, queueing drain execution...")
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

	if err := drainNode(ctx, logger, kube, lease, config); err != nil {
		return fmt.Errorf("error draining node, err: %w", err)
	}

	logger.Info("workload eviction finalized, issuing reboot...")

	if config.RebootDelay > 0 {
		logger.Info("delaying reboot for " + config.RebootDelay.String())
		time.Sleep(config.RebootDelay)
	}

	err = host.Reboot(ctx, cmder)
	if err != nil {
		return fmt.Errorf("failed to issue reboot command: %w", err)
	}

	return ErrRebootInitiated
}

func waitForNodeReady(ctx context.Context, kube kubernetes.Interface, nodeName string) (*corev1.Node, error) {
	timeoutCtx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		_, err := kube.Discovery().ServerVersion()
		if err == nil {
			node, err := kube.CoreV1().Nodes().Get(timeoutCtx, nodeName, metav1.GetOptions{})
			if err == nil {
				if isNodeReady(node) {
					return node, nil
				}
				slog.Info("network is up, but node is not ready yet, waiting...", "node", nodeName)
			} else {
				slog.Info("failed to get node status, waiting...", "node", nodeName, "error", err.Error())
			}
		} else {
			slog.Info("waiting for network/api-server...", "node", nodeName, "error", err.Error())
		}

		select {
		case <-timeoutCtx.Done():
			return nil, fmt.Errorf("timed out waiting for network and node readiness")
		case <-ticker.C:
			continue
		}
	}
}

func stagedDigest(status *host.BootcStatus) string {
	if status == nil || status.Staged == nil {
		return ""
	}
	return status.Staged.Image.ImageDigest
}

func isControlPlaneNode(node *corev1.Node) bool {
	if node == nil || node.Labels == nil {
		return false
	}
	if val, ok := node.Labels[ControlPlaneRoleLabel]; ok && val != "false" {
		return true
	}
	if val, ok := node.Labels[MasterRoleLabel]; ok && val != "false" {
		return true
	}
	return false
}

func isNodeReady(node *corev1.Node) bool {
	if node == nil {
		return false
	}
	for _, condition := range node.Status.Conditions {
		if condition.Type == corev1.NodeReady {
			return condition.Status == corev1.ConditionTrue
		}
	}
	return false
}

func setNodeAnnotation(ctx context.Context, kube kubernetes.Interface, nodeName, key, value string) error {
	patch, err := json.Marshal(map[string]any{
		"metadata": map[string]any{
			"annotations": map[string]any{
				key: value,
			},
		},
	})
	if err != nil {
		return err
	}
	_, err = kube.CoreV1().Nodes().Patch(ctx, nodeName, types.StrategicMergePatchType, patch, metav1.PatchOptions{})
	return err
}

func removeNodeAnnotation(ctx context.Context, kube kubernetes.Interface, nodeName, key string) error {
	patch, err := json.Marshal(map[string]any{
		"metadata": map[string]any{
			"annotations": map[string]any{
				key: nil,
			},
		},
	})
	if err != nil {
		return err
	}
	_, err = kube.CoreV1().Nodes().Patch(ctx, nodeName, types.StrategicMergePatchType, patch, metav1.PatchOptions{})
	return err
}

func areControlPlaneNodesDone(ctx context.Context, logger *slog.Logger, kube kubernetes.Interface, workerStagedAt time.Time) (bool, error) {
	nodes, err := kube.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return false, fmt.Errorf("failed to list nodes: %w", err)
	}

	minMasterCheckTime := workerStagedAt.Add(controlPlaneSyncGrace)

	for _, n := range nodes.Items {
		if !isControlPlaneNode(&n) {
			continue
		}

		if n.Spec.Unschedulable {
			logger.Debug("control plane node is unschedulable, waiting...", "control_plane_node", n.Name)
			return false, nil
		}

		if !isNodeReady(&n) {
			logger.Debug("control plane node is not ready, waiting...", "control_plane_node", n.Name)
			return false, nil
		}

		rawLastUpToDate := n.Annotations[AnnotationLastUpToDate]
		if rawLastUpToDate == "" {
			logger.Debug("control plane node has pending update or missing status annotation, waiting...", "control_plane_node", n.Name)
			return false, nil
		}

		lastUpToDate, err := time.Parse(time.RFC3339, rawLastUpToDate)
		if err != nil {
			logger.Warn("control plane node has invalid last-up-to-date annotation, waiting...",
				"control_plane_node", n.Name,
				"value", rawLastUpToDate,
				"error", err,
			)
			return false, nil
		}

		if !lastUpToDate.After(minMasterCheckTime) {
			logger.Debug("waiting for control plane node to confirm up-to-date state after worker staged time",
				"control_plane_node", n.Name,
				"master_last_up_to_date", lastUpToDate,
				"required_after", minMasterCheckTime,
			)
			return false, nil
		}
	}

	return true, nil
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

		// cluster should be considered "unhealthy" if a node is unschedulable
		if node.Spec.Unschedulable {
			return false, nil
		}

		if !isNodeReady(&node) {
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
				updatedLease, err := leases.Update(ctx, lease, metav1.UpdateOptions{})
				if err != nil {
					logger.Error("error refreshing lease", "error", err.Error())
				} else {
					lease = updatedLease // <-- Save the updated lease with the new resourceVersion
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
		Out:                 slogwriter.New(logger, slog.LevelDebug),
		ErrOut:              slogwriter.New(logger, slog.LevelError),
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
		return fmt.Errorf("failed to get node status: %w", err)
	}
	if !node.Spec.Unschedulable {
		// Node is already uncordoned
		return nil
	}
	logger.Info("target state validated. uncordoning node...")
	patch := []byte(`{"spec":{"unschedulable":false}}`)
	_, err = kube.CoreV1().Nodes().Patch(ctx, config.NodeName, types.StrategicMergePatchType, patch, metav1.PatchOptions{})
	return err
}

func getLease(ctx context.Context, kube kubernetes.Interface, config Config) (*coordinationv1.Lease, error) {
	leases := kube.CoordinationV1().Leases(config.LeaseNamespace)
	lease, err := leases.Get(ctx, config.LeaseName, metav1.GetOptions{})
	if k8serrors.IsNotFound(err) {
		return nil, nil
	}
	return lease, err
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
		createdLease, err := leases.Create(ctx, newLease, metav1.CreateOptions{})

		if k8serrors.IsAlreadyExists(err) {
			// we lost the race
			return nil, nil
		} else if err != nil {
			return nil, err
		}

		return createdLease, err
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

		updatedLease, err := leases.Update(ctx, lease, metav1.UpdateOptions{})
		if k8serrors.IsConflict(err) {
			// we lost the race
			return nil, nil
		} else if err != nil {
			return nil, err
		}

		return updatedLease, err
	}

	return nil, nil
}

func finalizeLeaseState(ctx context.Context, logger *slog.Logger, kube kubernetes.Interface, lease *coordinationv1.Lease, config Config) error {
	if config.CooldownDelay > 0 {
		logger.Info("entering cooldown phase to delay subsequent node upgrades", "delay", config.CooldownDelay.String())

		delayStr := fmt.Sprintf("cooldown:%s", config.NodeName)
		delaySecs := int32(config.CooldownDelay.Seconds())

		lease.Spec.HolderIdentity = &delayStr
		lease.Spec.RenewTime = &metav1.MicroTime{Time: config.Clock.Now()}
		lease.Spec.LeaseDurationSeconds = &delaySecs
	} else {
		logger.Info("releasing cluster upgrade lock object...")
		lease.Spec.HolderIdentity = nil
	}

	leases := kube.CoordinationV1().Leases(config.LeaseNamespace)
	_, err := leases.Update(ctx, lease, metav1.UpdateOptions{})
	if err != nil {
		return err
	}
	return nil
}
