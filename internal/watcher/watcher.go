package watcher

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/fsnotify/fsnotify"
)

func Monitor(dir string) error {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}
	defer watcher.Close()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)

	err = watcher.Add(dir)
	if err != nil {
		return err
	}

	go watch(watcher, sigCh)

	return nil
}

func watch(watcher *fsnotify.Watcher, sigCh chan os.Signal) {
	for {
		select {
		case _ = <-sigCh:
			return
		case event, ok := <-watcher.Events:
			if !ok {
				return
			}

			if event.Has(fsnotify.Create) {
				log.Printf("loading new config: %s", event.Name)
			}
		case err, ok := <-watcher.Errors:
			if !ok {
				return
			}
			log.Printf("watcher error: %v", err)
		}
	}
}
