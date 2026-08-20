package runner

import (
	"context"
	"fmt"
	"os"
	"time"
)

func pollCtx(ctx context.Context, sigCh chan os.Signal, cancel context.CancelFunc, interval time.Duration) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			if len(sigCh) > 0 {
				cancel()
				return
			}
			time.Sleep(interval)
		}
	}
}

func sleepWithContext(ctx context.Context, t time.Duration) error {
	select {
	case <-ctx.Done():
		return fmt.Errorf("interupt triggered")
	case <-time.After(t):
		return nil
	}
}
