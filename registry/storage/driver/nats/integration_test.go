package nats

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"io"
	"testing"

	"github.com/distribution/distribution/v3"
	dcontext "github.com/distribution/distribution/v3/internal/dcontext"
	"github.com/distribution/distribution/v3/manifest/schema2"
	"github.com/distribution/distribution/v3/registry/storage"
	"github.com/distribution/distribution/v3/registry/storage/driver"
	"github.com/distribution/reference"
	"github.com/opencontainers/go-digest"
	"github.com/opencontainers/image-spec/specs-go"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
)

// TestNATSIntegration provides comprehensive end-to-end testing
// of the NATS storage driver with full registry operations
func TestNATSIntegration(t *testing.T) {
	// Skip if running short tests
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := dcontext.Background()

	// Setup embedded NATS server and driver
	ns, nc, cleanup := startEmbeddedNATS(t)
	defer cleanup()
	_ = ns

	config := Config{
		ServerURL: nc.ConnectedUrl(),
		Bucket:    "registry-integration-test",
	}

	natsDriver, err := New(ctx, config)
	if err != nil {
		t.Fatalf("failed to create NATS driver: %v", err)
	}

	// Setup CLI helper
	cliHelper := newNATSCLIHelper(t, nc.ConnectedUrl(), config.Bucket)

	// Create registry with NATS driver
	registry := createTestRegistry(t, natsDriver)

	// Run test cases
	t.Run("SmallFile", func(t *testing.T) {
		testPushPullWorkflow(t, registry, cliHelper, generateTestData(t, 512*1024)) // 512KB
	})

	t.Run("LargeFile", func(t *testing.T) {
		testPushPullWorkflow(t, registry, cliHelper, generateTestData(t, 10*1024*1024)) // 10MB
	})

	t.Run("ComplexManifest", func(t *testing.T) {
		testComplexManifestWorkflow(t, registry, cliHelper)
	})

	t.Run("ConcurrentOperations", func(t *testing.T) {
		testConcurrentOperations(t, registry, cliHelper)
	})
}

func createTestRegistry(t *testing.T, natsDriver driver.StorageDriver) distribution.Namespace {
	t.Helper()

	ctx := dcontext.Background()

	// Create registry with NATS driver
	registry, err := storage.NewRegistry(ctx, natsDriver)
	if err != nil {
		t.Fatalf("failed to create registry: %v", err)
	}

	return registry
}

func testPushPullWorkflow(t *testing.T, registry distribution.Namespace, cliHelper *natsCLIHelper, testData []byte) {
	ctx := dcontext.Background()
	repoName := "test/integration"

	// Parse repository name
	namedRepo, err := reference.WithName(repoName)
	if err != nil {
		t.Fatalf("failed to parse repository name: %v", err)
	}

	// Get repository
	repo, err := registry.Repository(ctx, namedRepo)
	if err != nil {
		t.Fatalf("failed to get repository: %v", err)
	}

	// Push layer (blob)
	layerDigest := pushLayer(t, repo, testData)

	// Validate layer exists in NATS
	cliHelper.validateBlobExists(t, layerDigest, len(testData))

	// Create and push manifest
	manifestDigest := pushManifest(t, repo, layerDigest, int64(len(testData)))

	// Validate manifest exists in NATS
	cliHelper.validateManifestExists(t, manifestDigest)

	// Pull and verify layer
	pulledData := pullLayer(t, repo, layerDigest)
	if !bytes.Equal(testData, pulledData) {
		t.Errorf("pulled layer data doesn't match original")
	}

	// Pull and verify manifest
	pulledManifest := pullManifest(t, repo, manifestDigest)
	_, payload, err := pulledManifest.Payload()
	if err != nil {
		t.Fatalf("failed to get manifest payload: %v", err)
	}
	if len(payload) == 0 {
		t.Errorf("manifest payload is empty")
	}

	// Cross-validate with NATS CLI
	cliHelper.validateDataIntegrity(t, layerDigest, testData)
}

func testComplexManifestWorkflow(t *testing.T, registry distribution.Namespace, cliHelper *natsCLIHelper) {
	ctx := dcontext.Background()
	repoName := "test/complex"

	// Parse repository name
	namedRepo, err := reference.WithName(repoName)
	if err != nil {
		t.Fatalf("failed to parse repository name: %v", err)
	}

	repo, err := registry.Repository(ctx, namedRepo)
	if err != nil {
		t.Fatalf("failed to get repository: %v", err)
	}

	// Create multiple layers
	layers := []struct {
		data   []byte
		digest digest.Digest
	}{
		{generateTestData(t, 1024*1024), ""}, // 1MB
		{generateTestData(t, 2048*1024), ""}, // 2MB
		{generateTestData(t, 512*1024), ""},  // 512KB
	}

	// Push all layers
	for i := range layers {
		layers[i].digest = pushLayer(t, repo, layers[i].data)
		cliHelper.validateBlobExists(t, layers[i].digest, len(layers[i].data))
	}

	// First, push the config blob (empty JSON object)
	configData := []byte("{}")
	configDigest := pushLayer(t, repo, configData)

	// Create manifest with multiple layers
	configDescriptor := distribution.Descriptor{
		MediaType: "application/vnd.docker.container.image.v1+json",
		Size:      int64(len(configData)),
		Digest:    configDigest,
	}

	layerDescriptors := make([]distribution.Descriptor, len(layers))
	for i, layer := range layers {
		layerDescriptors[i] = distribution.Descriptor{
			MediaType: "application/vnd.docker.image.rootfs.diff.tar.gzip",
			Size:      int64(len(layer.data)),
			Digest:    layer.digest,
		}
	}

	manifest, err := schema2.FromStruct(schema2.Manifest{
		Versioned: specs.Versioned{
			SchemaVersion: 2,
		},
		Config: configDescriptor,
		Layers: layerDescriptors,
	})
	if err != nil {
		t.Fatalf("failed to create manifest: %v", err)
	}

	ms, err := repo.Manifests(ctx)
	if err != nil {
		t.Fatalf("failed to get manifest service: %v", err)
	}

	manifestDigest, err := ms.Put(ctx, manifest)
	if err != nil {
		t.Fatalf("failed to push manifest: %v", err)
	}

	// Validate all components exist in NATS
	cliHelper.validateManifestExists(t, manifestDigest)
	for _, layer := range layers {
		cliHelper.validateDataIntegrity(t, layer.digest, layer.data)
	}
}

func testConcurrentOperations(t *testing.T, registry distribution.Namespace, cliHelper *natsCLIHelper) {
	ctx := dcontext.Background()

	// Create multiple goroutines pushing different repositories
	numWorkers := 5
	results := make(chan error, numWorkers)

	for i := 0; i < numWorkers; i++ {
		go func(workerID int) {
			repoName := fmt.Sprintf("test/concurrent-%d", workerID)
			testData := generateTestData(t, 1024*1024) // 1MB per worker

			// Parse repository name
			namedRepo, err := reference.WithName(repoName)
			if err != nil {
				results <- fmt.Errorf("failed to parse repository name: %v", err)
				return
			}

			repo, err := registry.Repository(ctx, namedRepo)
			if err != nil {
				results <- fmt.Errorf("worker %d: failed to get repository: %v", workerID, err)
				return
			}

			// Push layer
			layerDigest := pushLayer(t, repo, testData)

			// Push manifest
			manifestDigest := pushManifest(t, repo, layerDigest, int64(len(testData)))

			// Validate in NATS
			if err := cliHelper.validateBlobExistsNonFatal(layerDigest, len(testData)); err != nil {
				results <- fmt.Errorf("worker %d: blob validation failed: %v", workerID, err)
				return
			}

			if err := cliHelper.validateManifestExistsNonFatal(manifestDigest); err != nil {
				results <- fmt.Errorf("worker %d: manifest validation failed: %v", workerID, err)
				return
			}

			results <- nil
		}(i)
	}

	// Wait for all workers to complete
	for i := 0; i < numWorkers; i++ {
		if err := <-results; err != nil {
			t.Errorf("concurrent operation failed: %v", err)
		}
	}
}

func pushLayer(t *testing.T, repo distribution.Repository, data []byte) digest.Digest {
	t.Helper()
	ctx := dcontext.Background()

	bs := repo.Blobs(ctx)

	// Calculate digest
	layerDigest := digest.FromBytes(data)

	// Push the layer
	writer, err := bs.Create(ctx)
	if err != nil {
		t.Fatalf("failed to create blob writer: %v", err)
	}

	n, err := writer.Write(data)
	if err != nil {
		_ = writer.Cancel(ctx)
		t.Fatalf("failed to write blob data: %v", err)
	}

	if n != len(data) {
		_ = writer.Cancel(ctx)
		t.Fatalf("incomplete write: wrote %d, expected %d", n, len(data))
	}

	_, err = writer.Commit(ctx, v1.Descriptor{
		Digest: layerDigest,
		Size:   int64(len(data)),
	})
	if err != nil {
		writer.Cancel(ctx)
		t.Fatalf("failed to commit blob: %v", err)
	}

	return layerDigest
}

func pushManifest(t *testing.T, repo distribution.Repository, layerDigest digest.Digest, layerSize int64) digest.Digest {
	t.Helper()
	ctx := dcontext.Background()

	// First, push the config blob (empty JSON object)
	configData := []byte("{}")
	configDigest := pushLayer(t, repo, configData)

	// Create a simple manifest with the actual config digest
	configDescriptor := distribution.Descriptor{
		MediaType: "application/vnd.docker.container.image.v1+json",
		Size:      int64(len(configData)),
		Digest:    configDigest,
	}

	layerDescriptor := distribution.Descriptor{
		MediaType: "application/vnd.docker.image.rootfs.diff.tar.gzip",
		Size:      layerSize,
		Digest:    layerDigest,
	}

	manifest, err := schema2.FromStruct(schema2.Manifest{
		Versioned: specs.Versioned{
			SchemaVersion: 2,
		},
		Config: configDescriptor,
		Layers: []distribution.Descriptor{layerDescriptor},
	})
	if err != nil {
		t.Fatalf("failed to create manifest: %v", err)
	}

	ms, err := repo.Manifests(ctx)
	if err != nil {
		t.Fatalf("failed to get manifest service: %v", err)
	}

	manifestDigest, err := ms.Put(ctx, manifest)
	if err != nil {
		t.Fatalf("failed to push manifest: %v", err)
	}

	return manifestDigest
}

func pullLayer(t *testing.T, repo distribution.Repository, layerDigest digest.Digest) []byte {
	t.Helper()
	ctx := dcontext.Background()

	bs := repo.Blobs(ctx)

	reader, err := bs.Open(ctx, layerDigest)
	if err != nil {
		t.Fatalf("failed to open blob for reading: %v", err)
	}
	defer reader.Close()

	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("failed to read blob data: %v", err)
	}

	return data
}

func pullManifest(t *testing.T, repo distribution.Repository, manifestDigest digest.Digest) distribution.Manifest {
	t.Helper()
	ctx := dcontext.Background()

	ms, err := repo.Manifests(ctx)
	if err != nil {
		t.Fatalf("failed to get manifest service: %v", err)
	}

	manifest, err := ms.Get(ctx, manifestDigest)
	if err != nil {
		t.Fatalf("failed to get manifest: %v", err)
	}

	return manifest
}

func generateTestData(t *testing.T, size int) []byte {
	t.Helper()

	data := make([]byte, size)
	_, err := rand.Read(data)
	if err != nil {
		t.Fatalf("failed to generate test data: %v", err)
	}

	return data
}
