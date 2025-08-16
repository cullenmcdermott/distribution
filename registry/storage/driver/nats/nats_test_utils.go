package nats

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/distribution/distribution/v3/registry/storage/driver"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

func startEmbeddedNATS(t *testing.T) (*server.Server, *nats.Conn, func()) {
	t.Helper()

	tmpDir, err := os.MkdirTemp("", "nats-test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	opts := &server.Options{
		JetStream: true,
		StoreDir:  tmpDir,
		Port:      -1, // Use random port
	}

	ns := server.New(opts)

	// Start server (non-blocking)
	ns.Start()

	// Wait for server to be ready for connections
	if !ns.ReadyForConnections(5 * time.Second) {
		ns.Shutdown()
		os.RemoveAll(tmpDir)
		t.Fatal("NATS server failed to become ready")
	}

	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		ns.Shutdown()
		os.RemoveAll(tmpDir)
		t.Fatalf("failed to connect to NATS: %v", err)
	}

	cleanup := func() {
		if nc != nil {
			nc.Close()
		}
		ns.Shutdown()
		ns.WaitForShutdown()
		os.RemoveAll(tmpDir)
	}

	return ns, nc, cleanup
}

func newTestDriverConstructor(t *testing.T) (driver.StorageDriver, func()) {
	t.Helper()

	_, nc, cleanup := startEmbeddedNATS(t)

	config := Config{
		ServerURL: nc.ConnectedUrl(),
		Bucket:    "test-bucket",
	}

	d, err := New(context.Background(), config)
	if err != nil {
		cleanup()
		t.Fatalf("failed to create driver: %v", err)
	}

	return d, cleanup
}
