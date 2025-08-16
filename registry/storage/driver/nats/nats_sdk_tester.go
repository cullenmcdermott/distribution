package nats

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/opencontainers/go-digest"
)

// NATSSDKTester provides direct NATS SDK integration for comprehensive testing
type NATSSDKTester struct {
	nc       *nats.Conn
	js       jetstream.JetStream
	os       jetstream.ObjectStore
	kv       jetstream.KeyValue
	bucket   string
	driver   *natsDriver
}

// NewNATSSDKTester creates a new NATS SDK tester connected to the same NATS instance as the driver
func NewNATSSDKTester(t *testing.T, driver *natsDriver) *NATSSDKTester {
	t.Helper()

	// Create a separate connection for testing (don't reuse driver's connection)
	config := driver.config
	nc, err := nats.Connect(config.ServerURL)
	if err != nil {
		t.Fatalf("failed to connect to NATS for testing: %v", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		t.Fatalf("failed to create JetStream context for testing: %v", err)
	}

	// Get the existing object store and KV store (don't create new ones)
	os, err := js.ObjectStore(context.Background(), config.Bucket)
	if err != nil {
		nc.Close()
		t.Fatalf("failed to get object store for testing: %v", err)
	}

	kv, err := js.KeyValue(context.Background(), config.Bucket+"_metadata")
	if err != nil {
		nc.Close()
		t.Fatalf("failed to get key-value store for testing: %v", err)
	}

	return &NATSSDKTester{
		nc:     nc,
		js:     js,
		os:     os,
		kv:     kv,
		bucket: config.Bucket,
		driver: driver,
	}
}

// Close closes the tester's NATS connection
func (tester *NATSSDKTester) Close() {
	if tester.nc != nil {
		tester.nc.Close()
	}
}

// ValidateBlobExists checks if a blob exists and has the correct size using NATS SDK
func (tester *NATSSDKTester) ValidateBlobExists(t *testing.T, blobDigest digest.Digest, expectedSize int) {
	t.Helper()
	err := tester.ValidateBlobExistsNonFatal(blobDigest, expectedSize)
	if err != nil {
		t.Fatal(err)
	}
}

// ValidateBlobExistsNonFatal checks if a blob exists and has the correct size (non-fatal version)
func (tester *NATSSDKTester) ValidateBlobExistsNonFatal(blobDigest digest.Digest, expectedSize int) error {
	ctx := context.Background()
	
	// Parse the digest to get the hash
	digestStr := blobDigest.String()
	if !strings.HasPrefix(digestStr, "sha256:") {
		return fmt.Errorf("unsupported digest algorithm: %s", digestStr)
	}
	hash := strings.TrimPrefix(digestStr, "sha256:")

	// Load metadata using the same approach as the driver
	path := fmt.Sprintf("/docker/registry/v2/blobs/sha256/%s/%s", hash[:2], hash)
	metadata, err := tester.driver.loadMetadata(ctx, path)
	if err != nil {
		return fmt.Errorf("blob metadata not found for %s: %v", blobDigest, err)
	}

	// Validate total size
	if metadata.TotalSize != int64(expectedSize) {
		return fmt.Errorf("blob %s size mismatch: expected %d, got %d", blobDigest, expectedSize, metadata.TotalSize)
	}

	// Validate that all chunks exist
	for i := 0; i < metadata.ChunkCount; i++ {
		chunkPath := chunkKey(hash, i)
		_, err := tester.os.GetInfo(ctx, chunkPath)
		if err != nil {
			return fmt.Errorf("chunk %d not found for blob %s: %v", i, blobDigest, err)
		}
	}

	return nil
}

// ValidateManifestExists checks if a manifest exists using NATS SDK
func (tester *NATSSDKTester) ValidateManifestExists(t *testing.T, manifestDigest digest.Digest) {
	t.Helper()
	err := tester.ValidateManifestExistsNonFatal(manifestDigest)
	if err != nil {
		t.Fatal(err)
	}
}

// ValidateManifestExistsNonFatal checks if a manifest exists (non-fatal version)
func (tester *NATSSDKTester) ValidateManifestExistsNonFatal(manifestDigest digest.Digest) error {
	ctx := context.Background()
	
	// Parse the digest to get the hash
	digestStr := manifestDigest.String()
	if !strings.HasPrefix(digestStr, "sha256:") {
		return fmt.Errorf("unsupported digest algorithm: %s", digestStr)
	}
	hash := strings.TrimPrefix(digestStr, "sha256:")

	// Load metadata for manifest
	path := fmt.Sprintf("/docker/registry/v2/repositories/_manifests/revisions/sha256/%s/%s", hash[:2], hash)
	_, err := tester.driver.loadMetadata(ctx, path)
	if err != nil {
		return fmt.Errorf("manifest metadata not found for %s: %v", manifestDigest, err)
	}

	return nil
}

// ValidateDataIntegrity verifies that the data stored in NATS matches the original data
func (tester *NATSSDKTester) ValidateDataIntegrity(t *testing.T, blobDigest digest.Digest, originalData []byte) {
	t.Helper()

	ctx := context.Background()
	
	// Parse the digest to get the hash
	digestStr := blobDigest.String()
	if !strings.HasPrefix(digestStr, "sha256:") {
		t.Fatalf("unsupported digest algorithm: %s", digestStr)
	}
	hash := strings.TrimPrefix(digestStr, "sha256:")

	// Load metadata
	path := fmt.Sprintf("/docker/registry/v2/blobs/sha256/%s/%s", hash[:2], hash)
	metadata, err := tester.driver.loadMetadata(ctx, path)
	if err != nil {
		t.Fatalf("failed to load metadata for %s: %v", blobDigest, err)
	}

	// Reconstruct data from chunks
	var reconstructedData []byte
	for i := 0; i < metadata.ChunkCount; i++ {
		chunkPath := chunkKey(hash, i)
		
		chunkReader, err := tester.os.Get(ctx, chunkPath)
		if err != nil {
			t.Fatalf("failed to get chunk %d for %s: %v", i, blobDigest, err)
		}
		
		chunkData, err := io.ReadAll(chunkReader)
		chunkReader.Close()
		if err != nil {
			t.Fatalf("failed to read chunk %d for %s: %v", i, blobDigest, err)
		}
		
		reconstructedData = append(reconstructedData, chunkData...)
	}

	// Compare with original data
	if !bytes.Equal(reconstructedData, originalData) {
		t.Errorf("data integrity check failed for %s: data doesn't match", blobDigest)
		t.Logf("Original size: %d, Reconstructed size: %d", len(originalData), len(reconstructedData))
	}

	// Verify checksum
	expectedChecksum := sha256.Sum256(originalData)
	actualChecksum := sha256.Sum256(reconstructedData)
	
	if !bytes.Equal(expectedChecksum[:], actualChecksum[:]) {
		t.Errorf("checksum mismatch for %s", blobDigest)
		t.Logf("Expected: %x", expectedChecksum)
		t.Logf("Actual: %x", actualChecksum)
	}
}

// ValidateChunkStructure verifies that chunks are stored correctly
func (tester *NATSSDKTester) ValidateChunkStructure(t *testing.T, blobDigest digest.Digest, expectedChunks int) {
	t.Helper()

	ctx := context.Background()
	
	// Parse the digest to get the hash
	digestStr := blobDigest.String()
	if !strings.HasPrefix(digestStr, "sha256:") {
		t.Fatalf("unsupported digest algorithm: %s", digestStr)
	}
	hash := strings.TrimPrefix(digestStr, "sha256:")

	// Load metadata
	path := fmt.Sprintf("/docker/registry/v2/blobs/sha256/%s/%s", hash[:2], hash)
	metadata, err := tester.driver.loadMetadata(ctx, path)
	if err != nil {
		t.Fatalf("failed to load metadata for %s: %v", blobDigest, err)
	}

	// Validate chunk count
	if metadata.ChunkCount != expectedChunks {
		t.Errorf("expected %d chunks for blob %s, found %d", expectedChunks, blobDigest, metadata.ChunkCount)
	}

	// Validate that all chunks exist and have correct sizes (using chunk metadata)
	for i := 0; i < metadata.ChunkCount; i++ {
		chunkPath := chunkKey(hash, i)
		
		info, err := tester.os.GetInfo(ctx, chunkPath)
		if err != nil {
			t.Errorf("chunk %d not found for blob %s: %v", i, blobDigest, err)
			continue
		}
		
		// Load chunk metadata to verify size
		chunkMeta, err := tester.driver.loadChunkMetadata(ctx, hash, i)
		if err != nil {
			t.Errorf("chunk metadata %d not found for blob %s: %v", i, blobDigest, err)
			continue
		}
		
		if info.Size != uint64(chunkMeta.Size) {
			t.Errorf("chunk %d size mismatch for blob %s: expected %d, got %d", 
				i, blobDigest, chunkMeta.Size, info.Size)
		}
	}
}

// ListAllObjects lists all objects in the NATS object store
func (tester *NATSSDKTester) ListAllObjects(t *testing.T) []string {
	t.Helper()

	ctx := context.Background()
	objects, err := tester.os.List(ctx)
	if err != nil {
		t.Fatalf("failed to list objects: %v", err)
	}

	var objectNames []string
	for _, obj := range objects {
		objectNames = append(objectNames, obj.Name)
	}

	return objectNames
}

// ListAllMetadata lists all metadata keys in the NATS KV store
func (tester *NATSSDKTester) ListAllMetadata(t *testing.T) []string {
	t.Helper()

	ctx := context.Background()
	keys, err := tester.kv.Keys(ctx)
	if err != nil {
		t.Fatalf("failed to list metadata keys: %v", err)
	}

	return keys
}

// GetObjectSize returns the size of an object in the NATS object store
func (tester *NATSSDKTester) GetObjectSize(t *testing.T, objectName string) uint64 {
	t.Helper()

	ctx := context.Background()
	info, err := tester.os.GetInfo(ctx, objectName)
	if err != nil {
		t.Fatalf("failed to get object info for %s: %v", objectName, err)
	}

	return info.Size
}

// ValidateObjectCount verifies the total number of objects stored
func (tester *NATSSDKTester) ValidateObjectCount(t *testing.T, expectedCount int) {
	t.Helper()

	objects := tester.ListAllObjects(t)
	if len(objects) != expectedCount {
		t.Errorf("expected %d objects, found %d", expectedCount, len(objects))
		t.Logf("Objects: %v", objects)
	}
}