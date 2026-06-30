package watcher

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"
	"wgrot/v2/internal/pool"

	"github.com/fsnotify/fsnotify"
)

func Monitor(pool *pool.Pool) error {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}
	defer watcher.Close()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)

	err = watcher.Add(pool.Dir)
	if err != nil {
		return err
	}

	go watch(watcher, pool, sigCh)
	return nil
}

func watch(watcher *fsnotify.Watcher, pool *pool.Pool, sigCh chan os.Signal) {
	for {
		select {
		case _ = <-sigCh:
			return
		case event, ok := <-watcher.Events:
			if !ok {
				fmt.Printf("event not ok")
				continue
			}

			if event.Has(fsnotify.Create) {
				fmt.Printf("loading new config: %s", event.Name)
				time.Sleep(1 * time.Second) // give time for file operations to complete

				pool.Append(event.Name)
			}
		case err, ok := <-watcher.Errors:
			if !ok {
				fmt.Printf("error not ok")
				continue
			}
			fmt.Printf("watcher error: %v", err)
		}
	}
}
