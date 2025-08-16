package nats

import (
	"context"
	"testing"

	"github.com/distribution/distribution/v3/registry/storage/driver"
)

func TestWriter(t *testing.T) {
	d, cleanup := newTestDriverConstructor(t)
	defer cleanup()

	natsDriver := d.(*natsDriver)

	t.Run("write and commit", func(t *testing.T) {
		ctx := context.Background()
		path := "/test/file.txt"
		content := []byte("hello world")

		writer, err := natsDriver.Writer(ctx, path, false)
		if err != nil {
			t.Fatalf("failed to create writer: %v", err)
		}

		n, err := writer.Write(content)
		if err != nil {
			t.Fatalf("failed to write content: %v", err)
		}

		if n != len(content) {
			t.Errorf("expected to write %d bytes, wrote %d", len(content), n)
		}

		if writer.Size() != int64(len(content)) {
			t.Errorf("expected size %d, got %d", len(content), writer.Size())
		}

		err = writer.Commit(ctx)
		if err != nil {
			t.Fatalf("failed to commit: %v", err)
		}

		// Verify content was written
		readContent, err := natsDriver.GetContent(ctx, path)
		if err != nil {
			t.Fatalf("failed to read content: %v", err)
		}

		if string(readContent) != string(content) {
			t.Errorf("expected content %q, got %q", string(content), string(readContent))
		}
	})

	t.Run("write and cancel", func(t *testing.T) {
		ctx := context.Background()
		path := "/test/cancelled.txt"
		content := []byte("this should be cancelled")

		writer, err := natsDriver.Writer(ctx, path, false)
		if err != nil {
			t.Fatalf("failed to create writer: %v", err)
		}

		_, err = writer.Write(content)
		if err != nil {
			t.Fatalf("failed to write content: %v", err)
		}

		err = writer.Cancel(ctx)
		if err != nil {
			t.Fatalf("failed to cancel: %v", err)
		}

		// Verify content was not written
		_, err = natsDriver.GetContent(ctx, path)
		if err == nil {
			t.Error("expected file to not exist after cancel")
		}
	})

	t.Run("append not supported", func(t *testing.T) {
		ctx := context.Background()
		path := "/test/append.txt"

		_, err := natsDriver.Writer(ctx, path, true)
		if err == nil {
			t.Error("expected error for append=true")
		}

		// Should be ErrUnsupportedMethod
		if _, ok := err.(driver.ErrUnsupportedMethod); !ok {
			t.Errorf("expected ErrUnsupportedMethod, got %T: %v", err, err)
		}
	})

	t.Run("write after close", func(t *testing.T) {
		ctx := context.Background()
		path := "/test/writeafterbClose.txt"

		writer, err := natsDriver.Writer(ctx, path, false)
		if err != nil {
			t.Fatalf("failed to create writer: %v", err)
		}

		err = writer.Close()
		if err != nil {
			t.Fatalf("failed to close: %v", err)
		}

		_, err = writer.Write([]byte("should fail"))
		if err == nil {
			t.Error("expected error when writing after close")
		}
	})

	t.Run("large content streaming", func(t *testing.T) {
		ctx := context.Background()
		path := "/test/large.txt"

		// Create large content (1MB)
		largeContent := make([]byte, 1024*1024)
		for i := range largeContent {
			largeContent[i] = byte(i % 256)
		}

		writer, err := natsDriver.Writer(ctx, path, false)
		if err != nil {
			t.Fatalf("failed to create writer: %v", err)
		}

		// Write in chunks to test streaming
		chunkSize := 64 * 1024
		for i := 0; i < len(largeContent); i += chunkSize {
			end := i + chunkSize
			if end > len(largeContent) {
				end = len(largeContent)
			}

			_, err = writer.Write(largeContent[i:end])
			if err != nil {
				t.Fatalf("failed to write chunk: %v", err)
			}
		}

		err = writer.Commit(ctx)
		if err != nil {
			t.Fatalf("failed to commit: %v", err)
		}

		// Verify content
		readContent, err := natsDriver.GetContent(ctx, path)
		if err != nil {
			t.Fatalf("failed to read content: %v", err)
		}

		if len(readContent) != len(largeContent) {
			t.Errorf("expected length %d, got %d", len(largeContent), len(readContent))
		}

		// Check a few bytes to ensure content is correct
		for i := 0; i < 1000; i++ {
			if readContent[i] != largeContent[i] {
				t.Errorf("content mismatch at byte %d: expected %d, got %d", i, largeContent[i], readContent[i])
				break
			}
		}
	})
}

// TestUploadSession tests the upload session protocol handling
func TestUploadSession(t *testing.T) {
	d, cleanup := newTestDriverConstructor(t)
	defer cleanup()

	natsDriver := d.(*natsDriver)
	ctx := context.Background()

	// Simulate Docker registry upload session paths
	repository := "hello-world"

	t.Run("upload session path detection", func(t *testing.T) {
		uploadID := "test-upload-path-detection"
		dataPath := "/docker/registry/v2/repositories/" + repository + "/_uploads/" + uploadID + "/data"
		hashstatesPath := "/docker/registry/v2/repositories/" + repository + "/_uploads/" + uploadID + "/hashstates/sha256/0"
		// Test path detection functions
		router := NewPathRouter()

		dataPathInfo := router.ParsePath(dataPath)
		if dataPathInfo.Type != PathTypeUploadData {
			t.Errorf("expected %s to be detected as data path", dataPath)
		}

		hashstatesPathInfo := router.ParsePath(hashstatesPath)
		if hashstatesPathInfo.Type != PathTypeUploadHashstates {
			t.Errorf("expected %s to be detected as hashstates path", hashstatesPath)
		}

		if dataPathInfo.Type == PathTypeUploadHashstates {
			t.Errorf("expected %s to NOT be detected as hashstates path", dataPath)
		}

		if hashstatesPathInfo.Type == PathTypeUploadData {
			t.Errorf("expected %s to NOT be detected as data path", hashstatesPath)
		}
	})

	t.Run("data path size tracking", func(t *testing.T) {
		uploadID := "test-upload-size-tracking"
		dataPath := "/docker/registry/v2/repositories/" + repository + "/_uploads/" + uploadID + "/data"
		// Write blob data to data path
		blobData := []byte("this is test blob data for upload session")

		dataWriter, err := natsDriver.Writer(ctx, dataPath, false)
		if err != nil {
			t.Fatalf("failed to create data writer: %v", err)
		}

		n, err := dataWriter.Write(blobData)
		if err != nil {
			t.Fatalf("failed to write blob data: %v", err)
		}

		if n != len(blobData) {
			t.Errorf("expected to write %d bytes, wrote %d", len(blobData), n)
		}

		err = dataWriter.Commit(ctx)
		if err != nil {
			t.Fatalf("failed to commit data: %v", err)
		}

		// Verify size tracking via Stat
		fileInfo, err := natsDriver.Stat(ctx, dataPath)
		if err != nil {
			t.Fatalf("failed to stat data path: %v", err)
		}

		if fileInfo.Size() != int64(len(blobData)) {
			t.Errorf("expected size %d, got %d", len(blobData), fileInfo.Size())
		}
	})

	t.Run("hashstates path separate storage", func(t *testing.T) {
		uploadID := "test-upload-hashstates"
		hashstatesPath := "/docker/registry/v2/repositories/" + repository + "/_uploads/" + uploadID + "/hashstates/sha256/0"
		// Write hashstates data to hashstates path
		hashstatesData := []byte("small hashstates data")

		hashstatesWriter, err := natsDriver.Writer(ctx, hashstatesPath, false)
		if err != nil {
			t.Fatalf("failed to create hashstates writer: %v", err)
		}

		n, err := hashstatesWriter.Write(hashstatesData)
		if err != nil {
			t.Fatalf("failed to write hashstates data: %v", err)
		}

		if n != len(hashstatesData) {
			t.Errorf("expected to write %d bytes, wrote %d", len(hashstatesData), n)
		}

		err = hashstatesWriter.Commit(ctx)
		if err != nil {
			t.Fatalf("failed to commit hashstates: %v", err)
		}

		// Verify hashstates size tracking
		hashstatesInfo, err := natsDriver.Stat(ctx, hashstatesPath)
		if err != nil {
			t.Fatalf("failed to stat hashstates path: %v", err)
		}

		if hashstatesInfo.Size() != int64(len(hashstatesData)) {
			t.Errorf("expected hashstates size %d, got %d", len(hashstatesData), hashstatesInfo.Size())
		}
	})

	t.Run("upload session append support", func(t *testing.T) {
		uploadID := "test-upload-append"
		dataPath := "/docker/registry/v2/repositories/" + repository + "/_uploads/" + uploadID + "/data"
		// Create initial upload session
		initialData := []byte("initial upload data")

		dataWriter, err := natsDriver.Writer(ctx, dataPath, false)
		if err != nil {
			t.Fatalf("failed to create initial data writer: %v", err)
		}

		_, err = dataWriter.Write(initialData)
		if err != nil {
			t.Fatalf("failed to write initial data: %v", err)
		}

		err = dataWriter.Commit(ctx)
		if err != nil {
			t.Fatalf("failed to commit initial data: %v", err)
		}

		// Append more data
		appendData := []byte(" appended data")

		appendWriter, err := natsDriver.Writer(ctx, dataPath, true)
		if err != nil {
			t.Fatalf("failed to create append writer: %v", err)
		}

		_, err = appendWriter.Write(appendData)
		if err != nil {
			t.Fatalf("failed to write append data: %v", err)
		}

		err = appendWriter.Commit(ctx)
		if err != nil {
			t.Fatalf("failed to commit append data: %v", err)
		}

		// Verify final size
		finalInfo, err := natsDriver.Stat(ctx, dataPath)
		if err != nil {
			t.Fatalf("failed to stat final data path: %v", err)
		}

		expectedSize := int64(len(initialData) + len(appendData))
		if finalInfo.Size() != expectedSize {
			t.Errorf("expected final size %d, got %d", expectedSize, finalInfo.Size())
			t.Logf("Initial data size: %d, append data size: %d", len(initialData), len(appendData))
		}
	})
}
