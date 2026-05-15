package host

import (
	"context"

	"github.com/elliot-gustafsson/kbootcd/internal/command"
)

func runOnHost(ctx context.Context, cmder command.Commander, args ...string) ([]byte, error) {
	nsenterArgs := append([]string{
		"-t", "1", // target pid 1
		"-m", // enter mount namespace
		"-u", // enter hosts UTS namespace
		"-i", // enter the hosts IPC namespace
		"-n", // enter the hosts network namespace
		"-p", // enter the hosts PID namespace
		"--",
	}, args...)

	return cmder.Run(ctx, "nsenter", nsenterArgs...)
}

func Reboot(ctx context.Context, cmder command.Commander) error {
	_, err := runOnHost(ctx, cmder, "systemctl", "reboot")
	return err
}
