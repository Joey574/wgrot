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

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)

	err = watcher.Add(pool.Dir)
	if err != nil {
		watcher.Close()
		return err
	}

	go watch(watcher, pool, sigCh)
	return nil
}

func watch(watcher *fsnotify.Watcher, pool *pool.Pool, sigCh chan os.Signal) {
	defer watcher.Close()

	for {
		select {
		case _ = <-sigCh:
			fmt.Println("recieved interrupt, exitting")
			return
		case event, ok := <-watcher.Events:
			if !ok {
				fmt.Println("channel closed, exiting...")
				return
			}

			if event.Has(fsnotify.Create) {
				fmt.Printf("loading new config: %s\n", event.Name)
				time.Sleep(1 * time.Second) // give time for file operations to complete

				pool.Append(event.Name)
			}
		case err, ok := <-watcher.Errors:
			if !ok {
				fmt.Println("channel closed, exiting...")
				return
			}
			fmt.Printf("watcher error: %v\n", err)
		}
	}
}
