package main

import (
	"flag"
	"log"
	"os"
	"time"
	"wgrot/v2/internal/pool"
	"wgrot/v2/internal/runner"
	"wgrot/v2/internal/sink"
	"wgrot/v2/internal/state"
	"wgrot/v2/internal/watcher"
)

func main() {
	iface := flag.String(
		"iface",
		"",
		"wireguard interface name",
	)

	poolDir := flag.String(
		"pool",
		"/etc/wgrot-pool",
		"directory of wireguard config pool",
	)

	workDir := flag.String(
		"workDir",
		"/etc/wgrot-pool",
		"work directory for handling cross-instance coordination",
	)

	refresh := flag.Duration(
		"refresh",
		3*time.Hour,
		"rotation interval",
	)

	verify := flag.Duration(
		"verify",
		30*time.Second,
		"verify interval",
	)

	timeout := flag.Duration(
		"timeout",
		15*time.Second,
		"handshake timeout",
	)

	skipRefresh := flag.Bool(
		"skip-refresh",
		false,
		"skip rotating on interval",
	)

	portForward := flag.Bool(
		"port-forward",
		false,
		"enable Proton NAT-PMP port forwarding",
	)

	publishPort := flag.String(
		"publish-port",
		"",
		"publish the current forwarded port to the provided file",
	)

	flag.Parse()

	sink.SetLogLevel(sink.DEBUG)
	sink.PushSinks(os.Stdout)
	sink.SetFormat("[\\t] *")

	pool := pool.NewPool(*poolDir)
	if err := pool.Load(); err != nil {
		log.Fatalf("loading pool: %v\n", err)
	}
	sink.Printf(sink.INFO, "loaded %d configs from %s\n", pool.Count(), *poolDir)

	if err := watcher.Monitor(pool); err != nil {
		log.Fatalf("monitoring directory: %v", err)
	}
	sink.Printf(sink.INFO, "monitoring %s for new configs\n", *poolDir)

	state := state.NewState(*workDir)

	runner := runner.NewRunner(
		state,
		pool,
		*iface,
		*refresh,
		*verify,
		*timeout,
		*portForward,
		*publishPort,
	)

	runner.Start(*skipRefresh)
}
