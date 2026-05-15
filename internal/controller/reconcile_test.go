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
}

func TestReconcileWithUpToDateDigest(t *testing.T) {
	mockCmder := mock_command.NewMockCommander(t)

	node := &corev1.Node{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "Node",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: "test001",
		},
		Spec: corev1.NodeSpec{
			Unschedulable: false,
		},
	}

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

	node := &corev1.Node{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "Node",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: "test001",
		},
		Spec: corev1.NodeSpec{
			Unschedulable: false,
		},
	}

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

	config := NewTestConfig("test001", time.Now())
	tImage := "registry.example.com/os:v2.1.0@sha256:2222222222222222222222222222222222222222222222222222222222222222"
	config.TargetImageConfigPath = createTempFile(t, tImage)

	err := Reconcile(context.Background(), mockCmder, kubeMockClient, config)
	assert.NoError(t, err)
}

func TestReconcileWithUpToDateTag(t *testing.T) {
	mockCmder := mock_command.NewMockCommander(t)

	node := &corev1.Node{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "Node",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: "test001",
		},
		Spec: corev1.NodeSpec{
			Unschedulable: false,
		},
	}

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

	node := &corev1.Node{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "Node",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: "test001",
		},
		Spec: corev1.NodeSpec{
			Unschedulable: false,
		},
	}

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
	assert.NoError(t, err)
}

func TestReconcileWithLeaseConflict(t *testing.T) {
	mockCmder := mock_command.NewMockCommander(t)

	holder := "test002"
	objects := []runtime.Object{
		&corev1.Node{
			TypeMeta: metav1.TypeMeta{
				APIVersion: "v1",
				Kind:       "Node",
			},
			ObjectMeta: metav1.ObjectMeta{
				Name: "test001",
			},
			Spec: corev1.NodeSpec{
				Unschedulable: false,
			},
		},
		&coordinationv1.Lease{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "bootc-upgrade-lock-test",
				Namespace: "test",
			},
			Spec: coordinationv1.LeaseSpec{
				HolderIdentity: &holder,
				AcquireTime:    &metav1.MicroTime{Time: time.Now()},
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
	config := NewTestConfig("test001", time.Date(2001, 1, 1, 0, 0, 0, 0, time.UTC))
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
			name: "overnight window - inside early morning (after midnight)",
			// 02:00 Sunday. Because it's an overnight window starting on Sat/Sun,
			// this actually counts as the "Saturday" maintenance window.
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
			name: "overnight window - correct time but day not allowed",
			// It is 23:00 on Friday. We only allow Sat/Sun.
			now:    friday.Add(23 * time.Hour),
			start:  "22:00",
			end:    "04:00",
			days:   []string{"Sat", "Sun"},
			reboot: false,
		},
		{
			name: "overnight window - Monday 02:00 wrap check",
			// It is 02:00 on MONDAY.
			// Because it is in the "morning wrap" phase, the logic checks if YESTERDAY
			// (Sunday) was allowed. Since Sunday IS allowed, this should pass!
			now:    monday.Add(2 * time.Hour),
			start:  "22:00",
			end:    "04:00",
			days:   []string{"Sat", "Sun"},
			reboot: true,
		},
		{
			name: "overnight window - Saturday 02:00 wrap check",
			// It is 02:00 on SATURDAY.
			// Morning wrap phase -> Checks if Friday is allowed.
			// Friday is NOT allowed. Should fail!
			now:    saturday.Add(2 * time.Hour),
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
			name: "overnight window - inside early morning (after midnight)",
			// 02:00 Sunday. Because it's an overnight window starting on Sat/Sun,
			// this actually counts as the "Saturday" maintenance window.
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			mockCmder := mock_command.NewMockCommander(t)

			node := &corev1.Node{
				TypeMeta: metav1.TypeMeta{
					APIVersion: "v1",
					Kind:       "Node",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name: "test001",
				},
				Spec: corev1.NodeSpec{
					Unschedulable: false,
				},
			}

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
			assert.NoError(t, err)

			err = Reconcile(context.Background(), mockCmder, kubeMockClient, config)
			assert.NoError(t, err)
		})
	}

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

func NewTestConfig(nodeName string, now time.Time) Config {
	// w, err := buildTimeWindow()

	return Config{
		NodeName:       nodeName,
		LeaseName:      "bootc-upgrade-lock-test",
		LeaseNamespace: "test",
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
