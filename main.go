package main

import (
	"flag"
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
	interval := flag.Duration("interval", 3*time.Hour, "rotation interval")
	timeout := flag.Duration("timeout", 15*time.Second, "handshake timeout")
	flag.Parse()

	pool := pool.NewPool()
	if err := pool.Load(*poolDir); err != nil {
		log.Fatalf("loading pool: %v\n", err)
	}
	log.Printf("loaded %d configs from %s", len(pool.Peers), *poolDir)

	if err := watcher.Monitor(*poolDir); err != nil {
		log.Fatalf("monitoring directory: %v", err)
	}
	log.Printf("monitoring %s for new configs", *poolDir)

	state := state.NewState(*statePath)
	state.Load(pool)

	runner.Start(state, pool, *iface, *interval, *timeout)
}
