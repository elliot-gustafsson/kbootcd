package controller

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/elliot-gustafsson/kbootcd/internal/host"
	"github.com/elliot-gustafsson/kbootcd/mocks/mock_command"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
)

func TestMain(m *testing.M) {
	opts := &slog.HandlerOptions{
		Level: slog.LevelError,
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, opts)))
	os.Exit(m.Run())
}

func TestReconcileWithUpToDateDigest(t *testing.T) {
	mockCmder := mock_command.NewMockCommander(t)

	node := newReadyNode("test001", nil, nil)
	kubeMockClient := fake.NewSimpleClientset(node)

	bootcStatus := MockBootcHost

	mockCmder.EXPECT().Run(mock.Anything, "nsenter", append(nsenterOpts, "bootc", "status", "--json")).
		Once().
		Return(toJson(t, bootcStatus), nil)

	config := NewTestConfig("test001", time.Now())
	tImage := "registry.example.com/os:v2.1.0@sha256:1111111111111111111111111111111111111111111111111111111111111111"
	config.TargetImageConfigPath = createTempFile(t, tImage)

	err := Reconcile(context.Background(), mockCmder, kubeMockClient, config)
	assert.NoError(t, err)
}

func TestReconcileWithOutOfDateDigest(t *testing.T) {
	mockCmder := mock_command.NewMockCommander(t)

	now := time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC)
	node := newReadyNode("test001", nil, nil)
	kubeMockClient := fake.NewSimpleClientset(node)

	bootcStatus := MockBootcHost

	mockCmder.EXPECT().Run(mock.Anything, "nsenter", append(nsenterOpts, "bootc", "status", "--json")).
		Once().
		Return(toJson(t, bootcStatus), nil)

	mockCmder.EXPECT().Run(mock.Anything, "nsenter", append(nsenterOpts, "bootc", "switch", "registry.example.com/os:v2.1.0@sha256:2222222222222222222222222222222222222222222222222222222222222222")).
		Once().
		Return([]byte{}, nil)

	updatedStatus := bootcStatus
	updatedStatus.Status.Staged = &MockStagedState

	mockCmder.EXPECT().Run(mock.Anything, "nsenter", append(nsenterOpts, "bootc", "status", "--json")).
		Once().
		Return(toJson(t, updatedStatus), nil)

	mockCmder.EXPECT().Run(mock.Anything, "nsenter", append(nsenterOpts, "systemctl", "reboot")).
		Once().
		Return([]byte{}, nil)

	config := NewTestConfig("test001", now)
	tImage := "registry.example.com/os:v2.1.0@sha256:2222222222222222222222222222222222222222222222222222222222222222"
	config.TargetImageConfigPath = createTempFile(t, tImage)

	err := Reconcile(context.Background(), mockCmder, kubeMockClient, config)
	assert.ErrorIs(t, err, ErrRebootInitiated)

	updatedNode, err := kubeMockClient.CoreV1().Nodes().Get(context.Background(), "test001", metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, now.Format(time.RFC3339), updatedNode.Annotations[AnnotationStagedAt])
}

func TestReconcileWithUpToDateTag(t *testing.T) {
	mockCmder := mock_command.NewMockCommander(t)

	node := newReadyNode("test001", nil, nil)
	kubeMockClient := fake.NewSimpleClientset(node)

	bootcStatus := MockBootcHost

	mockCmder.EXPECT().Run(mock.Anything, "nsenter", append(nsenterOpts, "bootc", "status", "--json")).
		Times(2).
		Return(toJson(t, bootcStatus), nil)

	mockCmder.EXPECT().Run(mock.Anything, "nsenter", append(nsenterOpts, "bootc", "switch", "registry.example.com/os:v2.1.0")).
		Once().
		Return([]byte{}, nil)

	config := NewTestConfig("test001", time.Now())
	tImage := "registry.example.com/os:v2.1.0"
	config.TargetImageConfigPath = createTempFile(t, tImage)

	err := Reconcile(context.Background(), mockCmder, kubeMockClient, config)
	assert.NoError(t, err)
}

func TestReconcileWithOutOfDateTag(t *testing.T) {
	mockCmder := mock_command.NewMockCommander(t)

	node := newReadyNode("test001", nil, nil)
	kubeMockClient := fake.NewSimpleClientset(node)

	bootcStatus := MockBootcHost

	mockCmder.EXPECT().Run(mock.Anything, "nsenter", append(nsenterOpts, "bootc", "status", "--json")).
		Once().
		Return(toJson(t, bootcStatus), nil)

	mockCmder.EXPECT().Run(mock.Anything, "nsenter", append(nsenterOpts, "bootc", "switch", "registry.example.com/os:v2.1.0")).
		Once().
		Return([]byte{}, nil)

	updatedStatus := bootcStatus
	updatedStatus.Status.Staged = &MockStagedState

	mockCmder.EXPECT().Run(mock.Anything, "nsenter", append(nsenterOpts, "bootc", "status", "--json")).
		Once().
		Return(toJson(t, updatedStatus), nil)

	mockCmder.EXPECT().Run(mock.Anything, "nsenter", append(nsenterOpts, "systemctl", "reboot")).
		Once().
		Return([]byte{}, nil)

	config := NewTestConfig("test001", time.Now())
	tImage := "registry.example.com/os:v2.1.0"
	config.TargetImageConfigPath = createTempFile(t, tImage)

	err := Reconcile(context.Background(), mockCmder, kubeMockClient, config)
	assert.ErrorIs(t, err, ErrRebootInitiated)
}

func TestReconcileWithLeaseConflict(t *testing.T) {
	mockCmder := mock_command.NewMockCommander(t)

	now := time.Date(2001, 1, 1, 0, 0, 0, 0, time.UTC)
	holder := "test002"
	leaseDuration := int32(3600)
	objects := []runtime.Object{
		newReadyNode("test001", nil, nil),
		&coordinationv1.Lease{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "bootc-upgrade-lock-test",
				Namespace: "test",
			},
			Spec: coordinationv1.LeaseSpec{
				HolderIdentity:       &holder,
				AcquireTime:          &metav1.MicroTime{Time: now},
				RenewTime:            &metav1.MicroTime{Time: now},
				LeaseDurationSeconds: &leaseDuration,
			},
		},
	}

	kubeMockClient := fake.NewSimpleClientset(objects...)

	bootcStatus := MockBootcHost

	mockCmder.EXPECT().Run(mock.Anything, "nsenter", append(nsenterOpts, "bootc", "status", "--json")).
		Once().
		Return(toJson(t, bootcStatus), nil)

	mockCmder.EXPECT().Run(mock.Anything, "nsenter", append(nsenterOpts, "bootc", "switch", "registry.example.com/os:v2.1.0")).
		Once().
		Return([]byte{}, nil)

	updatedStatus := bootcStatus
	updatedStatus.Status.Staged = &MockStagedState

	mockCmder.EXPECT().Run(mock.Anything, "nsenter", append(nsenterOpts, "bootc", "status", "--json")).
		Once().
		Return(toJson(t, updatedStatus), nil)

	tImage := "registry.example.com/os:v2.1.0"
	config := NewTestConfig("test001", now)
	config.TargetImageConfigPath = createTempFile(t, tImage)

	err := Reconcile(context.Background(), mockCmder, kubeMockClient, config)
	assert.NoError(t, err)
}

func TestReconcileWithMaintenanceWindow(t *testing.T) {

	monday := time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC)
	friday := time.Date(1, 1, 5, 0, 0, 0, 0, time.UTC)
	saturday := time.Date(1, 1, 6, 0, 0, 0, 0, time.UTC)
	sunday := time.Date(1, 1, 7, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		name   string
		now    time.Time
		start  string
		end    string
		days   []string
		reboot bool
	}{
		{
			name:   "always open",
			now:    time.Now(),
			start:  "00:00:00",
			end:    "23:59:59",
			days:   EveryDay,
			reboot: true,
		},
		{
			name:   "daytime window - before start",
			now:    monday.Add(10 * time.Hour), // 10:00
			start:  "12:00",
			end:    "18:00",
			days:   []string{"Mon", "Tue"},
			reboot: false,
		},
		{
			name:   "daytime window - inside window",
			now:    monday.Add(13 * time.Hour), // 13:00
			start:  "12:00",
			end:    "18:00",
			days:   []string{"Mon", "Tue"},
			reboot: true,
		},
		{
			name:   "daytime window - after end",
			now:    monday.Add(19 * time.Hour), // 19:00
			start:  "12:00",
			end:    "18:00",
			days:   []string{"Mon", "Tue"},
			reboot: false,
		},
		{
			name:   "daytime window - correct time, wrong day",
			now:    friday.Add(13 * time.Hour), // 13:00 Friday
			start:  "12:00",
			end:    "18:00",
			days:   []string{"Mon", "Tue"},
			reboot: false,
		},
		{
			name:   "overnight window - before start",
			now:    saturday.Add(20 * time.Hour), // 20:00 Sat
			start:  "22:00",
			end:    "04:00",
			days:   []string{"Sat", "Sun"},
			reboot: false,
		},
		{
			name:   "overnight window - inside late night (before midnight)",
			now:    saturday.Add(23 * time.Hour), // 23:00 Sat
			start:  "22:00",
			end:    "04:00",
			days:   []string{"Sat", "Sun"},
			reboot: true,
		},
		{
			name:   "overnight window - inside early morning (after midnight)",
			now:    sunday.Add(2 * time.Hour),
			start:  "22:00",
			end:    "04:00",
			days:   []string{"Sat", "Sun"},
			reboot: true,
		},
		{
			name:   "overnight window - after end (morning)",
			now:    sunday.Add(5 * time.Hour), // 05:00 Sun
			start:  "22:00",
			end:    "04:00",
			days:   []string{"Sat", "Sun"},
			reboot: false,
		},
		{
			name:   "overnight window - correct time but day not allowed",
			now:    friday.Add(23 * time.Hour),
			start:  "22:00",
			end:    "04:00",
			days:   []string{"Sat", "Sun"},
			reboot: false,
		},
		{
			name:   "overnight window - Monday 02:00 wrap check",
			now:    monday.Add(2 * time.Hour),
			start:  "22:00",
			end:    "04:00",
			days:   []string{"Sat", "Sun"},
			reboot: true,
		},
		{
			name:   "overnight window - Saturday 02:00 wrap check",
			now:    saturday.Add(2 * time.Hour),
			start:  "22:00",
			end:    "04:00",
			days:   []string{"Sat", "Sun"},
			reboot: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockCmder := mock_command.NewMockCommander(t)

			node := newReadyNode("test001", nil, nil)
			kubeMockClient := fake.NewSimpleClientset(node)

			bootcStatus := MockBootcHost

			mockCmder.EXPECT().Run(mock.Anything, "nsenter", append(nsenterOpts, "bootc", "status", "--json")).
				Once().
				Return(toJson(t, bootcStatus), nil)

			mockCmder.EXPECT().Run(mock.Anything, "nsenter", append(nsenterOpts, "bootc", "switch", "registry.example.com/os:v2.1.0@sha256:2222222222222222222222222222222222222222222222222222222222222222")).
				Once().
				Return([]byte{}, nil)

			updatedStatus := bootcStatus
			updatedStatus.Status.Staged = &MockStagedState

			mockCmder.EXPECT().Run(mock.Anything, "nsenter", append(nsenterOpts, "bootc", "status", "--json")).
				Once().
				Return(toJson(t, updatedStatus), nil)

			if tt.reboot {
				mockCmder.EXPECT().Run(mock.Anything, "nsenter", append(nsenterOpts, "systemctl", "reboot")).
					Once().
					Return([]byte{}, nil)
			}

			config := NewTestConfig("test001", tt.now)
			tImage := "registry.example.com/os:v2.1.0@sha256:2222222222222222222222222222222222222222222222222222222222222222"
			config.TargetImageConfigPath = createTempFile(t, tImage)
			var err error
			config.Window, err = BuildTimeWindow(tt.start, tt.end, tt.days, "UTC")
			require.NoError(t, err)

			err = Reconcile(context.Background(), mockCmder, kubeMockClient, config)
			if tt.reboot {
				assert.ErrorIs(t, err, ErrRebootInitiated)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestControlPlaneSetsAndRemovesLastUpToDateAnnotation(t *testing.T) {
	ctx := context.Background()
	t1 := time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC)
	t2 := t1.Add(5 * time.Minute)

	cpLabels := map[string]string{ControlPlaneRoleLabel: ""}
	master := newReadyNode("master-1", cpLabels, nil)
	kubeClient := fake.NewSimpleClientset(master)

	// Tick 1: Master is up-to-date -> sets kbootcd/last-up-to-date
	mockCmder1 := mock_command.NewMockCommander(t)
	mockCmder1.EXPECT().Run(mock.Anything, "nsenter", append(nsenterOpts, "bootc", "status", "--json")).
		Once().
		Return(toJson(t, MockBootcHost), nil)

	cfg := NewTestConfig("master-1", t1)
	cfg.TargetImageConfigPath = createTempFile(t, "registry.example.com/os:v2.1.0@sha256:1111111111111111111111111111111111111111111111111111111111111111")

	err := Reconcile(ctx, mockCmder1, kubeClient, cfg)
	require.NoError(t, err)

	gotMaster, err := kubeClient.CoreV1().Nodes().Get(ctx, "master-1", metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, t1.Format(time.RFC3339), gotMaster.Annotations[AnnotationLastUpToDate])

	// Tick 2: Master stages a new update -> removes kbootcd/last-up-to-date and reboots
	mockCmder2 := mock_command.NewMockCommander(t)
	newTarget := "registry.example.com/os:v2.2.0@sha256:2222222222222222222222222222222222222222222222222222222222222222"
	stagedHost := MockBootcHost
	stagedHost.Status.Staged = &MockStagedState

	mockCmder2.EXPECT().Run(mock.Anything, "nsenter", append(nsenterOpts, "bootc", "status", "--json")).
		Once().
		Return(toJson(t, MockBootcHost), nil)
	mockCmder2.EXPECT().Run(mock.Anything, "nsenter", append(nsenterOpts, "bootc", "switch", newTarget)).
		Once().
		Return([]byte{}, nil)
	mockCmder2.EXPECT().Run(mock.Anything, "nsenter", append(nsenterOpts, "bootc", "status", "--json")).
		Once().
		Return(toJson(t, stagedHost), nil)
	mockCmder2.EXPECT().Run(mock.Anything, "nsenter", append(nsenterOpts, "systemctl", "reboot")).
		Once().
		Return([]byte{}, nil)

	cfg2 := NewTestConfig("master-1", t2)
	cfg2.TargetImageConfigPath = createTempFile(t, newTarget)

	err = Reconcile(ctx, mockCmder2, kubeClient, cfg2)
	require.ErrorIs(t, err, ErrRebootInitiated)

	gotMaster, err = kubeClient.CoreV1().Nodes().Get(ctx, "master-1", metav1.GetOptions{})
	require.NoError(t, err)
	_, exists := gotMaster.Annotations[AnnotationLastUpToDate]
	assert.False(t, exists, "expected last-up-to-date annotation to be removed when reboot is pending")
}

func TestWorkerWaitsForControlPlaneBeforeAcquiringLease(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC)

	cpLabels := map[string]string{ControlPlaneRoleLabel: "true"}
	master1 := newReadyNode("master-1", cpLabels, map[string]string{
		// Checked 1 minute BEFORE worker noticed update -> stale relative to worker
		AnnotationLastUpToDate: baseTime.Add(-1 * time.Minute).Format(time.RFC3339),
	})
	master2 := newReadyNode("master-2", cpLabels, nil) // No annotation yet (e.g. upgrading)
	worker := newReadyNode("worker-1", nil, nil)

	kubeClient := fake.NewSimpleClientset(master1, master2, worker)

	workerTarget := "registry.example.com/worker-os:latest"
	stagedHost := MockBootcHost
	stagedHost.Status.Staged = &MockStagedState

	// Step 1 (12:00): Worker stages update in parallel, sets staged-at, but does NOT acquire lease or reboot
	cmder1 := mock_command.NewMockCommander(t)
	cmder1.EXPECT().Run(mock.Anything, "nsenter", append(nsenterOpts, "bootc", "status", "--json")).
		Once().
		Return(toJson(t, MockBootcHost), nil)
	cmder1.EXPECT().Run(mock.Anything, "nsenter", append(nsenterOpts, "bootc", "switch", workerTarget)).
		Once().
		Return([]byte{}, nil)
	cmder1.EXPECT().Run(mock.Anything, "nsenter", append(nsenterOpts, "bootc", "status", "--json")).
		Once().
		Return(toJson(t, stagedHost), nil)

	cfg1 := NewTestConfig("worker-1", baseTime)
	cfg1.TargetImageConfigPath = createTempFile(t, workerTarget)

	err := Reconcile(ctx, cmder1, kubeClient, cfg1)
	require.NoError(t, err)

	gotWorker, err := kubeClient.CoreV1().Nodes().Get(ctx, "worker-1", metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, baseTime.Format(time.RFC3339), gotWorker.Annotations[AnnotationStagedAt])

	// Verify worker did NOT grab the lease
	lease, err := getLease(ctx, kubeClient, cfg1)
	require.NoError(t, err)
	assert.Nil(t, lease)

	// Step 2 (12:01): Both masters have timestamps, but within the 2m grace window -> worker still waits and keeps original staged-at
	require.NoError(t, setNodeAnnotation(ctx, kubeClient, "master-1", AnnotationLastUpToDate, baseTime.Add(90*time.Second).Format(time.RFC3339)))
	require.NoError(t, setNodeAnnotation(ctx, kubeClient, "master-2", AnnotationLastUpToDate, baseTime.Add(90*time.Second).Format(time.RFC3339)))

	cmder2 := mock_command.NewMockCommander(t)
	cmder2.EXPECT().Run(mock.Anything, "nsenter", append(nsenterOpts, "bootc", "status", "--json")).
		Times(2).
		Return(toJson(t, stagedHost), nil)
	cmder2.EXPECT().Run(mock.Anything, "nsenter", append(nsenterOpts, "bootc", "switch", workerTarget)).
		Once().
		Return([]byte{}, nil)

	cfg2 := NewTestConfig("worker-1", baseTime.Add(3*time.Minute))
	cfg2.TargetImageConfigPath = cfg1.TargetImageConfigPath

	err = Reconcile(ctx, cmder2, kubeClient, cfg2)
	require.NoError(t, err)

	gotWorker, err = kubeClient.CoreV1().Nodes().Get(ctx, "worker-1", metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, baseTime.Format(time.RFC3339), gotWorker.Annotations[AnnotationStagedAt], "staged-at must stay frozen on waiting ticks even with tag target")

	// Step 3 (12:05): All masters confirm up-to-date AFTER worker.staged-at + 2m -> worker acquires lease and reboots
	require.NoError(t, setNodeAnnotation(ctx, kubeClient, "master-1", AnnotationLastUpToDate, baseTime.Add(4*time.Minute).Format(time.RFC3339)))
	require.NoError(t, setNodeAnnotation(ctx, kubeClient, "master-2", AnnotationLastUpToDate, baseTime.Add(4*time.Minute).Format(time.RFC3339)))

	cmder3 := mock_command.NewMockCommander(t)
	cmder3.EXPECT().Run(mock.Anything, "nsenter", append(nsenterOpts, "bootc", "status", "--json")).
		Times(2).
		Return(toJson(t, stagedHost), nil)
	cmder3.EXPECT().Run(mock.Anything, "nsenter", append(nsenterOpts, "bootc", "switch", workerTarget)).
		Once().
		Return([]byte{}, nil)
	cmder3.EXPECT().Run(mock.Anything, "nsenter", append(nsenterOpts, "systemctl", "reboot")).
		Once().
		Return([]byte{}, nil)

	cfg3 := NewTestConfig("worker-1", baseTime.Add(5*time.Minute))
	cfg3.TargetImageConfigPath = cfg1.TargetImageConfigPath

	err = Reconcile(ctx, cmder3, kubeClient, cfg3)
	require.ErrorIs(t, err, ErrRebootInitiated)

	// Step 4 (12:10): Worker finishes rebooting into target image -> uncordons, releases lease, and removes staged-at
	bootedNewHost := host.BootcHost{
		APIVersion: "org.containers.bootc/v1",
		Kind:       "BootcHost",
		Status: host.BootcStatus{
			Booted: MockStagedState,
		},
	}

	cmder4 := mock_command.NewMockCommander(t)
	cmder4.EXPECT().Run(mock.Anything, "nsenter", append(nsenterOpts, "bootc", "status", "--json")).
		Times(2).
		Return(toJson(t, bootedNewHost), nil)
	cmder4.EXPECT().Run(mock.Anything, "nsenter", append(nsenterOpts, "bootc", "switch", workerTarget)).
		Once().
		Return([]byte{}, nil)

	cfg4 := NewTestConfig("worker-1", baseTime.Add(10*time.Minute))
	cfg4.TargetImageConfigPath = cfg1.TargetImageConfigPath

	err = Reconcile(ctx, cmder4, kubeClient, cfg4)
	require.NoError(t, err)

	gotWorker, err = kubeClient.CoreV1().Nodes().Get(ctx, "worker-1", metav1.GetOptions{})
	require.NoError(t, err)
	_, stagedExists := gotWorker.Annotations[AnnotationStagedAt]
	assert.False(t, stagedExists, "expected staged-at annotation to be removed once worker is up-to-date")
	assert.False(t, gotWorker.Spec.Unschedulable, "expected worker to be uncordoned")
}

func TestWorkerResetsStagedAtWhenNewDigestIsStaged(t *testing.T) {
	ctx := context.Background()
	t0 := time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC)
	tHotfix := t0.Add(10 * time.Minute) // 12:10

	cpLabels := map[string]string{ControlPlaneRoleLabel: ""}
	master := newReadyNode("master-1", cpLabels, map[string]string{
		// Master checked at 12:08 (newer than t0, but OLDER than tHotfix at 12:10)
		AnnotationLastUpToDate: t0.Add(8 * time.Minute).Format(time.RFC3339),
	})
	worker := newReadyNode("worker-1", nil, map[string]string{
		AnnotationStagedAt: t0.Format(time.RFC3339),
	})

	kubeClient := fake.NewSimpleClientset(master, worker)

	alreadyStagedHost := MockBootcHost
	alreadyStagedHost.Status.Staged = &MockStagedState // sha256:2222...

	hotfixStagedState := host.BootcState{
		Image: host.StateImage{
			Image: host.ContainerImage{
				Image:     "registry.example.com/os:v2.2.1",
				Transport: "registry",
			},
			ImageDigest: "sha256:3333333333333333333333333333333333333333333333333333333333333333",
			Version:     "v2.2.1",
			Timestamp:   tHotfix,
		},
	}
	postHotfixHost := MockBootcHost
	postHotfixHost.Status.Staged = &hotfixStagedState

	hotfixTarget := "registry.example.com/os:v2.2.1@sha256:3333333333333333333333333333333333333333333333333333333333333333"

	cmder := mock_command.NewMockCommander(t)
	cmder.EXPECT().Run(mock.Anything, "nsenter", append(nsenterOpts, "bootc", "status", "--json")).
		Once().
		Return(toJson(t, alreadyStagedHost), nil)
	cmder.EXPECT().Run(mock.Anything, "nsenter", append(nsenterOpts, "bootc", "switch", hotfixTarget)).
		Once().
		Return([]byte{}, nil)
	cmder.EXPECT().Run(mock.Anything, "nsenter", append(nsenterOpts, "bootc", "status", "--json")).
		Once().
		Return(toJson(t, postHotfixHost), nil)

	cfg := NewTestConfig("worker-1", tHotfix)
	cfg.TargetImageConfigPath = createTempFile(t, hotfixTarget)

	err := Reconcile(ctx, cmder, kubeClient, cfg)
	require.NoError(t, err)

	gotWorker, err := kubeClient.CoreV1().Nodes().Get(ctx, "worker-1", metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, tHotfix.Format(time.RFC3339), gotWorker.Annotations[AnnotationStagedAt], "staged-at should be updated when staged SHA changes")
}

// ##########################################
// ############ end of tests ################
// ##########################################

var (
	nsenterOpts = []string{"-t", "1", "-m", "-u", "-i", "-n", "-p", "--"}

	// MockBootedState represents the base host structure
	MockBootcHost = host.BootcHost{
		APIVersion: "org.containers.bootc/v1",
		Kind:       "BootcHost",
		Status: host.BootcStatus{
			Booted: host.BootcState{
				Image: host.StateImage{
					Image: host.ContainerImage{
						Image:     "registry.example.com/os:v2.1.0",
						Transport: "registry",
					},
					ImageDigest: "sha256:1111111111111111111111111111111111111111111111111111111111111111",
					Version:     "v2.1.0",
					Timestamp:   time.Date(2001, 1, 2, 0, 0, 0, 0, time.UTC),
				},
			},
		},
	}

	// MockStagedState represents an update downloaded and waiting for reboot
	MockStagedState = host.BootcState{
		Image: host.StateImage{
			Image: host.ContainerImage{
				Image:     "registry.example.com/os:v2.2.0",
				Transport: "registry",
			},
			ImageDigest: "sha256:2222222222222222222222222222222222222222222222222222222222222222",
			Version:     "v2.2.0",
			Timestamp:   time.Date(2001, 1, 3, 0, 0, 0, 0, time.UTC),
		},
	}
	// MockRollbackState represents the previous OS layer available as a fallback
	MockRollbackState = host.BootcState{
		Image: host.StateImage{
			Image: host.ContainerImage{
				Image:     "registry.example.com/os:v2.0.0",
				Transport: "registry",
			},
			ImageDigest: "sha256:0000000000000000000000000000000000000000000000000000000000000000",
			Version:     "v2.0.0",
			Timestamp:   time.Date(2001, 1, 1, 0, 0, 0, 0, time.UTC),
		},
	}
)

type MockClock struct {
	now time.Time
}

func (c *MockClock) Now() time.Time {
	return c.now
}

func (c *MockClock) Since(t time.Time) time.Duration {
	return c.now.Sub(t)
}

func newReadyNode(name string, labels, annotations map[string]string) *corev1.Node {
	return &corev1.Node{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "Node",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Labels:      labels,
			Annotations: annotations,
		},
		Spec: corev1.NodeSpec{
			Unschedulable: false,
		},
		Status: corev1.NodeStatus{
			Conditions: []corev1.NodeCondition{
				{
					Type:   corev1.NodeReady,
					Status: corev1.ConditionTrue,
				},
			},
		},
	}
}

func NewTestConfig(nodeName string, now time.Time) Config {
	return Config{
		NodeName:       nodeName,
		LeaseName:      "bootc-upgrade-lock-test",
		LeaseNamespace: "test",
		LeaseDuration:  60 * time.Minute,
		Clock:          &MockClock{now},
		Window: Window{
			Start:    time.Time{},
			End:      time.Date(0, 0, 0, 23, 59, 59, 0, time.UTC),
			Weekdays: 127, // All days
			Location: time.UTC,
		},
	}
}

func createTempFile(t *testing.T, content string) string {
	tempDir := t.TempDir()

	f, err := os.CreateTemp(tempDir, strings.ReplaceAll(t.Name(), string(filepath.Separator), "_"))
	if err != nil {
		t.Fatalf("failed to write temp config file: %v", err)
	}

	f.WriteString(content)
	return f.Name()
}

func toJson(t *testing.T, data any) []byte {
	b, err := json.Marshal(data)
	if err != nil {
		t.Fatalf("error json marshaling, err: %s", err.Error())
	}
	return b
}
