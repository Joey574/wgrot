package state

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"wgrot/v2/internal/peer"
	"wgrot/v2/internal/pool"
	"wgrot/v2/internal/sink"

	"github.com/gofrs/flock"
)

const (
	spacing = 20 * time.Second
	jitter  = 10 * time.Second
)

type State struct {
	workDir  string
	lockPath string
	timePath string

	lastIdx  int
	lastPeer *peer.Peer

	lock *flock.Flock
}

func NewState(workDir string) *State {
	lockf := filepath.Join(workDir, "instances.lock")
	timef := filepath.Join(workDir, "instances.time")

	return &State{
		workDir:  workDir,
		lockPath: lockf,
		timePath: timef,

		lock: flock.New(lockf),
	}
}

func (s *State) Next(ctx context.Context, pool *pool.Pool) *peer.Peer {
	if pool.Count() == 0 {
		sink.Println(sink.WARN, "no availible peers in pool")
		return nil
	}

	s.lastIdx = min(max(s.lastIdx, 0), pool.Count())
	for {

		s.lastIdx = (s.lastIdx + 1) % pool.Count()
		p := pool.At(s.lastIdx)
		ok, err := p.TryLock()
		if ok && err == nil {
			if s.lastPeer != nil {
				if err = s.lastPeer.Unlock(); err != nil {
					sink.Printf(sink.ERROR, "failed to unlock peer: %v\n", err)
				}
			}

			s.lastPeer = p
			return p
		}

		if err != nil {
			sink.Printf(sink.ERROR, "failed to lock peer: %v\n", err)
		}

		if !ok {
			sink.Printf(sink.DEBUG, "peer already in use: '%s'\n", p.Name)
		}

		if err := sleepWithContext(ctx, time.Second); err != nil {
			return nil
		}
	}
}

func (s *State) WaitForConnection(ctx context.Context) error {
	ok, err := s.lock.TryLockContext(ctx, 100*time.Millisecond)
	if err != nil {
		return fmt.Errorf("instance lock: %w", err)
	}

	if !ok {
		return fmt.Errorf("interrupt triggered")
	}
	defer s.lock.Unlock()

	f, err := os.OpenFile(s.timePath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return fmt.Errorf("opening fleet timestamp file: %w", err)
	}
	defer f.Close()

	var last int64
	buf := make([]byte, 32)
	n, _ := f.ReadAt(buf, 0)
	if n > 0 {
		last, _ = strconv.ParseInt(strings.TrimSpace(string(buf[:n])), 10, 64)
	}

	if elapsed := time.Since(time.Unix(last, 0)); elapsed < spacing {
		wait := spacing - elapsed
		if jitter > 0 {
			wait += time.Duration(rand.Int63n(int64(jitter)))
		}

		sink.Printf(sink.DEBUG, "recent fleet rotation: waiting %s before next attempt\n", wait.String())
		if err := sleepWithContext(ctx, wait); err != nil {
			return err
		}
	}

	now := time.Now().Unix()
	if err := f.Truncate(0); err != nil {
		return fmt.Errorf("truncating fleet lock: %w", err)
	}

	if _, err := f.WriteAt([]byte(strconv.FormatInt(now, 10)), 0); err != nil {
		return fmt.Errorf("writing fleet lock: %w", err)
	}

	return nil
}
