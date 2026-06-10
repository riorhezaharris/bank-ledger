package config

import (
	"os"
	"strings"
	"time"
)

type Config struct {
	NodeID            string
	NodeAddr          string
	PeerAddrs         []string
	DatabaseURL       string
	HeartbeatInterval time.Duration
	HeartbeatTimeout  time.Duration
	PendingTTL        time.Duration
	WriteQuorum       int
}

func Load() Config {
	peers := []string{}
	if p := os.Getenv("PEER_ADDRS"); p != "" {
		for _, addr := range strings.Split(p, ",") {
			if addr = strings.TrimSpace(addr); addr != "" {
				peers = append(peers, addr)
			}
		}
	}

	return Config{
		NodeID:            env("NODE_ID", "node1"),
		NodeAddr:          env("NODE_ADDR", ":8080"),
		PeerAddrs:         peers,
		DatabaseURL:       env("DATABASE_URL", "postgres://ledger:ledger@localhost:5432/ledger?sslmode=disable"),
		HeartbeatInterval: 500 * time.Millisecond,
		HeartbeatTimeout:  2 * time.Second,
		PendingTTL:        5 * time.Second,
		WriteQuorum:       2, // W=2 out of N=3
	}
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
