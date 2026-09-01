package state

import (
	"context"
	"fmt"
	"time"
)

func sleepWithContext(ctx context.Context, t time.Duration) error {
	select {
	case <-ctx.Done():
		return fmt.Errorf("interupt triggered")
	case <-time.After(t):
		return nil
	}
}
