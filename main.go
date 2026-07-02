package main

import (
	"flag"
	"fmt"
	"log"
	"time"
	"wgrot/v2/internal/pool"
	"wgrot/v2/internal/runner"
	"wgrot/v2/internal/state"
	"wgrot/v2/internal/watcher"
)

func main() {
	iface := flag.String("iface", "", "wireguard interface name")
	poolDir := flag.String("pool", "/etc/wgrot-pool", "directory of wireguard config pool")
	statePath := flag.String("state", "", "state file path")
	refresh := flag.Duration("refresh", 3*time.Hour, "rotation interval")
	verify := flag.Duration("verify", 10*time.Second, "verify interval")
	timeout := flag.Duration("timeout", 15*time.Second, "handshake timeout")
	flag.Parse()

	pool := pool.NewPool(*poolDir)
	if err := pool.Load(); err != nil {
		log.Fatalf("loading pool: %v\n", err)
	}
	fmt.Printf("loaded %d configs from %s\n", pool.Count(), *poolDir)

	if err := watcher.Monitor(pool); err != nil {
		log.Fatalf("monitoring directory: %v", err)
	}
	fmt.Printf("monitoring %s for new configs\n", *poolDir)

	state := state.NewState(*statePath)
	state.Load(pool)

	runner.Start(state, pool, *iface, *refresh, *verify, *timeout)
}
