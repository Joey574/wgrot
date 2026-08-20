package watcher

import (
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
	"wgrot/v2/internal/pool"
	"wgrot/v2/internal/sink"

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

			if event.Has(fsnotify.Create) && strings.HasSuffix(event.Name, ".conf") {
				handleCreate(&event, pool)
			}

			if event.Has(fsnotify.Remove) && strings.HasSuffix(event.Name, ".conf") {
				handleRemove(&event, pool)
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

func handleCreate(event *fsnotify.Event, pool *pool.Pool) {
	sink.Printf(sink.DEBUG, "loading new config: %s\n", event.Name)
	time.Sleep(time.Second) // give time for event to complete
	pool.Append(event.Name)
}

func handleRemove(event *fsnotify.Event, pool *pool.Pool) {
	sink.Printf(sink.DEBUG, "removing config from pool: %s", event.Name)
	time.Sleep(time.Second) // give time for event to complete
	go pool.Remove(event.Name)
}
