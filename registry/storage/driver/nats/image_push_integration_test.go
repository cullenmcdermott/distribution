package nats

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/distribution/distribution/v3/configuration"
	"github.com/distribution/distribution/v3/registry/handlers"

	"github.com/google/go-containerregistry/pkg/crane"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/tarball"
)

// TestImagePushIntegration tests the complete OCI image push workflow
// using real container image libraries to validate the NATS storage driver
func TestImagePushIntegration(t *testing.T) {
	// Skip if running short tests
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()

	// Setup embedded NATS server
	ns, nc, cleanup := startEmbeddedNATS(t)
	defer cleanup()
	_ = ns

	// Create registry application using NewApp with NATS configuration
	appConfig := &configuration.Configuration{
		Storage: configuration.Storage{
			"nats": configuration.Parameters{
				"serverurl": nc.ConnectedUrl(),
				"bucket":    "registry-image-push-test",
			},
		},
		HTTP: configuration.HTTP{
			Prefix: "/",
		},
	}

	app := handlers.NewApp(ctx, appConfig)

	// Start HTTP test server
	server := httptest.NewServer(app)
	defer server.Close()

	t.Logf("Registry server started at: %s", server.URL)

	// TODO: Once go-containerregistry is added as dependency, implement image push using crane/remote
	// For now, create a simple test structure that we can expand

	t.Run("validate registry connectivity", func(t *testing.T) {
		// Test basic registry API endpoint
		resp, err := server.Client().Get(server.URL + "/v2/")
		if err != nil {
			t.Fatalf("Failed to connect to registry: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != 200 {
			t.Errorf("Registry /v2/ endpoint returned %d, expected 200", resp.StatusCode)
		}

		t.Logf("Registry API endpoint responding correctly")
	})

	t.Run("real OCI image push and pull", func(t *testing.T) {
		// Create a simple image using go-containerregistry
		img := empty.Image

		// Add a test layer
		layer, err := tarball.LayerFromOpener(func() (io.ReadCloser, error) {
			return io.NopCloser(strings.NewReader("Hello from NATS storage driver test layer!")), nil
		})
		if err != nil {
			t.Fatalf("Failed to create layer: %v", err)
		}

		img, err = mutate.AppendLayers(img, layer)
		if err != nil {
			t.Fatalf("Failed to append layer: %v", err)
		}

		// Create reference to our test registry
		registryHost := strings.TrimPrefix(server.URL, "http://")
		ref, err := name.ParseReference(fmt.Sprintf("%s/test/nats-image:latest", registryHost))
		if err != nil {
			t.Fatalf("Failed to parse reference: %v", err)
		}

		t.Logf("Pushing image to: %s", ref.String())

		// Push the image - this will exercise the full NATS driver workflow
		err = crane.Push(img, ref.String(), crane.Insecure)
		if err != nil {
			t.Fatalf("Failed to push image: %v", err)
		}

		t.Logf("Image pushed successfully!")

		// Verify the image can be pulled back
		t.Logf("Pulling image back to verify...")
		pulledImg, err := crane.Pull(ref.String(), crane.Insecure)
		if err != nil {
			t.Fatalf("Failed to pull image: %v", err)
		}

		// Verify layers match
		originalLayers, err := img.Layers()
		if err != nil {
			t.Fatalf("Failed to get original layers: %v", err)
		}

		pulledLayers, err := pulledImg.Layers()
		if err != nil {
			t.Fatalf("Failed to get pulled layers: %v", err)
		}

		if len(originalLayers) != len(pulledLayers) {
			t.Errorf("Layer count mismatch: original %d, pulled %d",
				len(originalLayers), len(pulledLayers))
		}

		// Verify layer content
		for i, originalLayer := range originalLayers {
			originalReader, err := originalLayer.Uncompressed()
			if err != nil {
				t.Fatalf("Failed to get original layer %d reader: %v", i, err)
			}
			defer originalReader.Close()

			originalContent, err := io.ReadAll(originalReader)
			if err != nil {
				t.Fatalf("Failed to read original layer %d: %v", i, err)
			}

			pulledReader, err := pulledLayers[i].Uncompressed()
			if err != nil {
				t.Fatalf("Failed to get pulled layer %d reader: %v", i, err)
			}
			defer pulledReader.Close()

			pulledContent, err := io.ReadAll(pulledReader)
			if err != nil {
				t.Fatalf("Failed to read pulled layer %d: %v", i, err)
			}

			if string(originalContent) != string(pulledContent) {
				t.Errorf("Layer %d content mismatch:\nOriginal: %q\nPulled: %q",
					i, string(originalContent), string(pulledContent))
			}
		}

		t.Logf("Image verification successful - NATS driver working correctly!")
	})

	t.Run("stress test multiple pushes", func(t *testing.T) {
		// Test multiple concurrent pushes to surface any race conditions
		numConcurrent := 3
		results := make(chan error, numConcurrent)

		for i := 0; i < numConcurrent; i++ {
			go func(index int) {
				// Create unique image for each push
				img := empty.Image

				layerContent := fmt.Sprintf("Concurrent test layer #%d content", index)
				layer, err := tarball.LayerFromOpener(func() (io.ReadCloser, error) {
					return io.NopCloser(strings.NewReader(layerContent)), nil
				})
				if err != nil {
					results <- fmt.Errorf("concurrent %d: failed to create layer: %v", index, err)
					return
				}

				img, err = mutate.AppendLayers(img, layer)
				if err != nil {
					results <- fmt.Errorf("concurrent %d: failed to append layer: %v", index, err)
					return
				}

				// Push to unique repository
				registryHost := strings.TrimPrefix(server.URL, "http://")
				ref, err := name.ParseReference(fmt.Sprintf("%s/test/concurrent-%d:latest", registryHost, index))
				if err != nil {
					results <- fmt.Errorf("concurrent %d: failed to parse reference: %v", index, err)
					return
				}

				err = crane.Push(img, ref.String(), crane.Insecure)
				if err != nil {
					results <- fmt.Errorf("concurrent %d: failed to push: %v", index, err)
					return
				}

				// Verify pull works
				_, err = crane.Pull(ref.String(), crane.Insecure)
				if err != nil {
					results <- fmt.Errorf("concurrent %d: failed to pull: %v", index, err)
					return
				}

				results <- nil // Success
			}(i)
		}

		// Wait for all pushes to complete
		for i := 0; i < numConcurrent; i++ {
			if err := <-results; err != nil {
				t.Errorf("Concurrent push error: %v", err)
			} else {
				t.Logf("Concurrent push %d completed successfully", i)
			}
		}
	})
}

// TestNATSDriverDirectErrors tests NATS driver error handling without HTTP layer
func TestNATSDriverDirectErrors(t *testing.T) {
	d, cleanup := newTestDriverConstructor(t)
	defer cleanup()

	natsDriver := d.(*natsDriver)
	ctx := context.Background()

	t.Run("upload session error handling", func(t *testing.T) {
		// Test the specific upload session workflow that causes issues
		repository := "test-repo"
		uploadID := "error-test"
		dataPath := "/docker/registry/v2/repositories/" + repository + "/_uploads/" + uploadID + "/data"

		testData := []byte("test data for error handling")

		// Create writer
		writer, err := natsDriver.Writer(ctx, dataPath, false)
		if err != nil {
			t.Fatalf("failed to create writer: %v", err)
		}

		// Write data
		n, err := writer.Write(testData)
		if err != nil {
			t.Fatalf("failed to write data: %v", err)
		}

		if n != len(testData) {
			t.Fatalf("expected to write %d bytes, wrote %d", len(testData), n)
		}

		// Check metadata state before commit
		metadata, err := natsDriver.loadMetadata(ctx, dataPath)
		if err != nil {
			t.Fatalf("failed to load metadata: %v", err)
		}

		t.Logf("Pre-commit metadata - TotalSize: %d, ChunkCount: %d", metadata.TotalSize, metadata.ChunkCount)

		// Try to read before commit - this exposes the issue
		reader, err := natsDriver.Reader(ctx, dataPath, 0)
		if err != nil {
			t.Fatalf("failed to create reader: %v", err)
		}
		defer reader.Close()

		readBuffer := make([]byte, len(testData))
		bytesRead, readErr := reader.Read(readBuffer)

		t.Logf("Read before commit: %d bytes, error: %v", bytesRead, readErr)

		// The issue manifests as reading 0 bytes when we expect data
		if bytesRead == 0 && metadata.TotalSize > 0 {
			t.Logf("ISSUE DETECTED: Reader returned 0 bytes despite TotalSize=%d", metadata.TotalSize)
			t.Logf("This is the root cause of digest validation failures")
		}

		// Commit the writer
		err = writer.Commit(ctx)
		if err != nil {
			t.Fatalf("failed to commit: %v", err)
		}

		// Check metadata state after commit
		metadata, err = natsDriver.loadMetadata(ctx, dataPath)
		if err != nil {
			t.Fatalf("failed to load metadata after commit: %v", err)
		}

		t.Logf("Post-commit metadata - TotalSize: %d, ChunkCount: %d", metadata.TotalSize, metadata.ChunkCount)

		// Try to read after commit
		reader2, err := natsDriver.Reader(ctx, dataPath, 0)
		if err != nil {
			t.Fatalf("failed to create reader after commit: %v", err)
		}
		defer reader2.Close()

		readBuffer2 := make([]byte, len(testData))
		bytesRead2, readErr2 := reader2.Read(readBuffer2)

		t.Logf("Read after commit: %d bytes, error: %v", bytesRead2, readErr2)

		if bytesRead2 != len(testData) {
			t.Errorf("After commit: expected %d bytes, got %d", len(testData), bytesRead2)
		}
	})
}

// TestRegistryDigestValidationWorkflow tests the exact workflow that was failing
// with the race condition: registry handlers performing digest validation during upload.
func TestRegistryDigestValidationWorkflow(t *testing.T) {
	// Skip if running short tests
	if testing.Short() {
		t.Skip("Skipping digest validation workflow test in short mode")
	}

	ctx := context.Background()

	// Setup embedded NATS server
	ns, nc, cleanup := startEmbeddedNATS(t)
	defer cleanup()
	_ = ns

	// Create registry application using NewApp with NATS configuration
	appConfig := &configuration.Configuration{
		Storage: configuration.Storage{
			"nats": configuration.Parameters{
				"serverurl": nc.ConnectedUrl(),
				"bucket":    "registry-digest-validation-test",
			},
		},
		HTTP: configuration.HTTP{
			Prefix: "/",
		},
	}

	app := handlers.NewApp(ctx, appConfig)

	// Start HTTP test server
	server := httptest.NewServer(app)
	defer server.Close()

	t.Logf("Registry server started at: %s", server.URL)

	t.Run("digest_validation_during_upload", func(t *testing.T) {
		// Simulate the exact sequence that was failing:
		// 1. Start blob upload
		// 2. Write blob data
		// 3. Registry validates digest by reading upload data
		// 4. Finalize upload with digest

		repositoryName := "test/digest-validation"
		blobData := []byte("test blob data for digest validation")
		blobDigest := fmt.Sprintf("sha256:%x", sha256.Sum256(blobData))

		client := server.Client()
		baseURL := server.URL + "/v2/" + repositoryName

		// Step 1: Start blob upload
		resp, err := client.Post(baseURL+"/blobs/uploads/", "application/json", nil)
		if err != nil {
			t.Fatalf("Failed to start blob upload: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != 202 {
			t.Fatalf("Expected 202 for upload start, got %d", resp.StatusCode)
		}

		// Get upload URL from Location header
		uploadURL := resp.Header.Get("Location")
		if uploadURL == "" {
			t.Fatalf("No upload URL in Location header")
		}

		// Make upload URL absolute
		if !strings.HasPrefix(uploadURL, "http") {
			uploadURL = server.URL + uploadURL
		}

		t.Logf("Upload URL: %s", uploadURL)

		// Step 2: Write blob data using PATCH
		patchReq, err := http.NewRequest("PATCH", uploadURL, bytes.NewReader(blobData))
		if err != nil {
			t.Fatalf("Failed to create PATCH request: %v", err)
		}
		patchReq.Header.Set("Content-Type", "application/octet-stream")
		patchReq.Header.Set("Content-Length", fmt.Sprintf("%d", len(blobData)))

		patchResp, err := client.Do(patchReq)
		if err != nil {
			t.Fatalf("Failed to PATCH blob data: %v", err)
		}
		defer patchResp.Body.Close()

		if patchResp.StatusCode != 202 {
			t.Fatalf("Expected 202 for PATCH, got %d", patchResp.StatusCode)
		}

		// Step 3: Finalize upload with digest (this triggers digest validation)
		// The registry will read back the uploaded data to validate the digest
		finalizeURL := patchResp.Header.Get("Location")
		if finalizeURL == "" {
			t.Fatalf("No finalize URL in PATCH response")
		}

		if !strings.HasPrefix(finalizeURL, "http") {
			finalizeURL = server.URL + finalizeURL
		}

		// Add digest parameter
		if strings.Contains(finalizeURL, "?") {
			finalizeURL += "&digest=" + url.QueryEscape(blobDigest)
		} else {
			finalizeURL += "?digest=" + url.QueryEscape(blobDigest)
		}

		t.Logf("Finalize URL: %s", finalizeURL)

		// Step 4: PUT to finalize (this was failing with race condition)
		putReq, err := http.NewRequest("PUT", finalizeURL, nil)
		if err != nil {
			t.Fatalf("Failed to create PUT request: %v", err)
		}
		putReq.Header.Set("Content-Length", "0")

		putResp, err := client.Do(putReq)
		if err != nil {
			t.Fatalf("Failed to PUT finalize: %v", err)
		}
		defer putResp.Body.Close()

		// This should succeed with our fix (was failing with DIGEST_INVALID before)
		if putResp.StatusCode != 201 {
			body, _ := io.ReadAll(putResp.Body)
			t.Fatalf("Expected 201 for PUT finalize, got %d. Response: %s",
				putResp.StatusCode, string(body))
		}

		t.Logf("SUCCESS: Digest validation during upload completed successfully")

		// Step 5: Verify blob can be retrieved
		blobURL := server.URL + "/v2/" + repositoryName + "/blobs/" + blobDigest
		getResp, err := client.Get(blobURL)
		if err != nil {
			t.Fatalf("Failed to GET blob: %v", err)
		}
		defer getResp.Body.Close()

		if getResp.StatusCode != 200 {
			t.Fatalf("Expected 200 for blob GET, got %d", getResp.StatusCode)
		}

		retrievedData, err := io.ReadAll(getResp.Body)
		if err != nil {
			t.Fatalf("Failed to read retrieved blob: %v", err)
		}

		if !bytes.Equal(blobData, retrievedData) {
			t.Fatalf("Retrieved blob data doesn't match original")
		}

		t.Logf("SUCCESS: Blob retrieval verified")
	})
}

// This integration test uses go-containerregistry to perform real OCI image push/pull
// operations against an embedded registry with NATS storage driver.
//
// The test validates:
// 1. Complete image push workflow (layers + manifest)
// 2. Image pull and verification
// 3. Concurrent operations
// 4. Driver-level error handling
//
// Any race conditions or digest validation issues in the NATS driver will
// surface as push/pull failures with detailed error messages.
