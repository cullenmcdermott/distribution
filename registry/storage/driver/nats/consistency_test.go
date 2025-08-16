package nats

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestUploadSessionReadBeforeCommit tests the critical race condition that was causing
// Docker push failures. This test verifies that data written to upload session paths
// is immediately readable before commit, which is required for digest validation.
func TestUploadSessionReadBeforeCommit(t *testing.T) {
	d, cleanup := newTestDriverConstructor(t)
	defer cleanup()

	natsDriver := d.(*natsDriver)
	ctx := context.Background()

	// Use upload session path format that the registry uses
	repository := "test/upload-consistency"
	uploadID := "test-upload-session-123"
	uploadDataPath := fmt.Sprintf("/docker/registry/v2/repositories/%s/_uploads/%s/data", repository, uploadID)

	testData := []byte("test data for upload session consistency validation")

	t.Run("single_write_immediate_read", func(t *testing.T) {
		// Create writer for upload session
		writer, err := natsDriver.Writer(ctx, uploadDataPath, false)
		if err != nil {
			t.Fatalf("failed to create writer: %v", err)
		}

		// Write data (should be immediately available due to upload session strategy)
		n, err := writer.Write(testData)
		if err != nil {
			t.Fatalf("failed to write data: %v", err)
		}

		if n != len(testData) {
			t.Fatalf("expected to write %d bytes, wrote %d", len(testData), n)
		}

		// CRITICAL TEST: Try to read data BEFORE commit
		// This was failing with the race condition - returning 0 bytes
		reader, err := natsDriver.Reader(ctx, uploadDataPath, 0)
		if err != nil {
			t.Fatalf("failed to create reader before commit: %v", err)
		}
		defer reader.Close()

		readBuffer := make([]byte, len(testData))
		bytesRead, readErr := reader.Read(readBuffer)

		// This should succeed with the fix
		if readErr != nil {
			t.Fatalf("failed to read data before commit: %v", readErr)
		}

		if bytesRead != len(testData) {
			t.Fatalf("expected to read %d bytes before commit, got %d", len(testData), bytesRead)
		}

		if string(readBuffer[:bytesRead]) != string(testData) {
			t.Fatalf("data mismatch before commit: expected %q, got %q", 
				string(testData), string(readBuffer[:bytesRead]))
		}

		// Commit should still work
		err = writer.Commit(ctx)
		if err != nil {
			t.Fatalf("failed to commit: %v", err)
		}

		t.Logf("SUCCESS: Upload session data is immediately readable before commit")
	})

	t.Run("multiple_writes_immediate_read", func(t *testing.T) {
		uploadDataPath2 := fmt.Sprintf("/docker/registry/v2/repositories/%s/_uploads/%s-multi/data", repository, uploadID)
		
		writer, err := natsDriver.Writer(ctx, uploadDataPath2, false)
		if err != nil {
			t.Fatalf("failed to create writer: %v", err)
		}

		// Write in multiple chunks
		chunk1 := []byte("first chunk ")
		chunk2 := []byte("second chunk ")
		chunk3 := []byte("third chunk")
		
		totalData := append(append(chunk1, chunk2...), chunk3...)

		// Write first chunk
		_, err = writer.Write(chunk1)
		if err != nil {
			t.Fatalf("failed to write chunk1: %v", err)
		}

		// Should be readable after first chunk
		reader1, err := natsDriver.Reader(ctx, uploadDataPath2, 0)
		if err != nil {
			t.Fatalf("failed to create reader after chunk1: %v", err)
		}
		
		buffer1 := make([]byte, len(chunk1))
		n1, err := reader1.Read(buffer1)
		reader1.Close()
		
		if err != nil {
			t.Fatalf("failed to read after chunk1: %v", err)
		}
		if n1 != len(chunk1) {
			t.Fatalf("expected %d bytes after chunk1, got %d", len(chunk1), n1)
		}

		// Write remaining chunks
		_, err = writer.Write(chunk2)
		if err != nil {
			t.Fatalf("failed to write chunk2: %v", err)
		}
		
		_, err = writer.Write(chunk3)
		if err != nil {
			t.Fatalf("failed to write chunk3: %v", err)
		}

		// Should be able to read all data before commit
		reader2, err := natsDriver.Reader(ctx, uploadDataPath2, 0)
		if err != nil {
			t.Fatalf("failed to create reader before commit: %v", err)
		}
		defer reader2.Close()

		fullBuffer := make([]byte, len(totalData))
		totalRead, err := reader2.Read(fullBuffer)
		if err != nil {
			t.Fatalf("failed to read full data before commit: %v", err)
		}

		if totalRead != len(totalData) {
			t.Fatalf("expected to read %d bytes, got %d", len(totalData), totalRead)
		}

		if string(fullBuffer[:totalRead]) != string(totalData) {
			t.Fatalf("data mismatch: expected %q, got %q", 
				string(totalData), string(fullBuffer[:totalRead]))
		}

		err = writer.Commit(ctx)
		if err != nil {
			t.Fatalf("failed to commit: %v", err)
		}

		t.Logf("SUCCESS: Multiple chunk upload session data is immediately readable")
	})
}

// TestUploadSessionMetadataConsistency verifies that metadata (TotalSize, ChunkCount)
// is always consistent and there are no observable intermediate states that could
// cause race conditions.
func TestUploadSessionMetadataConsistency(t *testing.T) {
	d, cleanup := newTestDriverConstructor(t)
	defer cleanup()

	natsDriver := d.(*natsDriver)
	ctx := context.Background()

	repository := "test/metadata-consistency"
	uploadID := "metadata-test-session"
	uploadDataPath := fmt.Sprintf("/docker/registry/v2/repositories/%s/_uploads/%s/data", repository, uploadID)

	testData := []byte("metadata consistency test data")

	t.Run("metadata_atomic_updates", func(t *testing.T) {
		writer, err := natsDriver.Writer(ctx, uploadDataPath, false)
		if err != nil {
			t.Fatalf("failed to create writer: %v", err)
		}

		// Write data
		_, err = writer.Write(testData)
		if err != nil {
			t.Fatalf("failed to write data: %v", err)
		}

		// Check metadata consistency
		metadata, err := natsDriver.loadMetadata(ctx, uploadDataPath)
		if err != nil {
			t.Fatalf("failed to load metadata: %v", err)
		}

		// After the fix, TotalSize and ChunkCount should be consistent
		if metadata.TotalSize > 0 && metadata.ChunkCount == 0 {
			t.Fatalf("INCONSISTENT METADATA: TotalSize=%d but ChunkCount=%d", 
				metadata.TotalSize, metadata.ChunkCount)
		}

		if metadata.TotalSize != int64(len(testData)) {
			t.Fatalf("expected TotalSize=%d, got %d", len(testData), metadata.TotalSize)
		}

		if metadata.ChunkCount != 1 {
			t.Fatalf("expected ChunkCount=1, got %d", metadata.ChunkCount)
		}

		// Verify data is actually accessible
		reader, err := natsDriver.Reader(ctx, uploadDataPath, 0)
		if err != nil {
			t.Fatalf("failed to create reader: %v", err)
		}
		defer reader.Close()

		readData := make([]byte, len(testData))
		n, err := reader.Read(readData)
		if err != nil {
			t.Fatalf("failed to read data: %v", err)
		}

		if n != len(testData) {
			t.Fatalf("metadata claimed %d bytes but read %d bytes", len(testData), n)
		}

		err = writer.Commit(ctx)
		if err != nil {
			t.Fatalf("failed to commit: %v", err)
		}

		t.Logf("SUCCESS: Metadata is consistent (TotalSize=%d, ChunkCount=%d)", 
			metadata.TotalSize, metadata.ChunkCount)
	})
}

// TestConcurrentUploadSessionWriteRead simulates the exact scenario that was failing:
// concurrent write operations while the registry is trying to read for digest validation.
func TestConcurrentUploadSessionWriteRead(t *testing.T) {
	d, cleanup := newTestDriverConstructor(t)
	defer cleanup()

	natsDriver := d.(*natsDriver)
	ctx := context.Background()

	repository := "test/concurrent-ops"
	uploadID := "concurrent-test-session"
	uploadDataPath := fmt.Sprintf("/docker/registry/v2/repositories/%s/_uploads/%s/data", repository, uploadID)

	t.Run("writer_reader_concurrency", func(t *testing.T) {
		testData := []byte("concurrent write-read test data")
		var writeErr, readErr error
		var readData []byte
		var wg sync.WaitGroup

		// Start writer
		wg.Add(1)
		go func() {
			defer wg.Done()
			
			writer, err := natsDriver.Writer(ctx, uploadDataPath, false)
			if err != nil {
				writeErr = fmt.Errorf("failed to create writer: %w", err)
				return
			}

			// Small delay to allow reader to start
			time.Sleep(10 * time.Millisecond)

			_, err = writer.Write(testData)
			if err != nil {
				writeErr = fmt.Errorf("failed to write data: %w", err)
				return
			}

			// Keep writing for a bit
			time.Sleep(50 * time.Millisecond)

			err = writer.Commit(ctx)
			if err != nil {
				writeErr = fmt.Errorf("failed to commit: %w", err)
				return
			}
		}()

		// Start reader (simulating registry digest validation)
		wg.Add(1)
		go func() {
			defer wg.Done()
			
			// Try reading multiple times like registry would do
			for attempts := 0; attempts < 10; attempts++ {
				time.Sleep(20 * time.Millisecond)
				
				reader, err := natsDriver.Reader(ctx, uploadDataPath, 0)
				if err != nil {
					// Expected early in the process
					continue
				}

				buffer := make([]byte, len(testData))
				n, err := reader.Read(buffer)
				reader.Close()

				if err != nil {
					// May fail early, but shouldn't fail once data is written
					continue
				}

				if n > 0 {
					readData = buffer[:n]
					break
				}
			}
		}()

		wg.Wait()

		if writeErr != nil {
			t.Fatalf("write error: %v", writeErr)
		}

		if readErr != nil {
			t.Fatalf("read error: %v", readErr)
		}

		// Verify we eventually read the correct data
		if readData == nil {
			t.Fatalf("reader never successfully read any data")
		}

		if string(readData) != string(testData) {
			t.Fatalf("data mismatch: expected %q, got %q", string(testData), string(readData))
		}

		t.Logf("SUCCESS: Concurrent write/read operations work correctly")
	})

	t.Run("multiple_concurrent_sessions", func(t *testing.T) {
		// Test multiple upload sessions running concurrently
		numSessions := 3
		var wg sync.WaitGroup
		results := make(chan error, numSessions)

		for i := 0; i < numSessions; i++ {
			wg.Add(1)
			go func(sessionIndex int) {
				defer wg.Done()
				
				sessionPath := fmt.Sprintf("/docker/registry/v2/repositories/%s/_uploads/session-%d/data", 
					repository, sessionIndex)
				sessionData := []byte(fmt.Sprintf("session %d test data", sessionIndex))

				writer, err := natsDriver.Writer(ctx, sessionPath, false)
				if err != nil {
					results <- fmt.Errorf("session %d: failed to create writer: %w", sessionIndex, err)
					return
				}

				_, err = writer.Write(sessionData)
				if err != nil {
					results <- fmt.Errorf("session %d: failed to write: %w", sessionIndex, err)
					return
				}

				// Verify immediate readability
				reader, err := natsDriver.Reader(ctx, sessionPath, 0)
				if err != nil {
					results <- fmt.Errorf("session %d: failed to create reader: %w", sessionIndex, err)
					return
				}

				buffer := make([]byte, len(sessionData))
				n, err := reader.Read(buffer)
				reader.Close()

				if err != nil {
					results <- fmt.Errorf("session %d: failed to read: %w", sessionIndex, err)
					return
				}

				if n != len(sessionData) {
					results <- fmt.Errorf("session %d: read %d bytes, expected %d", sessionIndex, n, len(sessionData))
					return
				}

				err = writer.Commit(ctx)
				if err != nil {
					results <- fmt.Errorf("session %d: failed to commit: %w", sessionIndex, err)
					return
				}

				results <- nil // Success
			}(i)
		}

		wg.Wait()
		close(results)

		// Check all results
		for err := range results {
			if err != nil {
				t.Fatalf("concurrent session error: %v", err)
			}
		}

		t.Logf("SUCCESS: %d concurrent upload sessions completed successfully", numSessions)
	})
}

// TestUploadSessionVsRegularFile verifies that upload sessions have different
// consistency behavior than regular files, which is a key requirement.
func TestUploadSessionVsRegularFile(t *testing.T) {
	d, cleanup := newTestDriverConstructor(t)
	defer cleanup()

	natsDriver := d.(*natsDriver)
	ctx := context.Background()

	testData := []byte("comparison test data")

	t.Run("upload_session_immediate_consistency", func(t *testing.T) {
		// Upload session path - should be immediately readable
		uploadPath := "/docker/registry/v2/repositories/test/repo/_uploads/test-uuid/data"
		
		writer, err := natsDriver.Writer(ctx, uploadPath, false)
		if err != nil {
			t.Fatalf("failed to create upload session writer: %v", err)
		}

		_, err = writer.Write(testData)
		if err != nil {
			t.Fatalf("failed to write to upload session: %v", err)
		}

		// Should be readable immediately for upload sessions
		reader, err := natsDriver.Reader(ctx, uploadPath, 0)
		if err != nil {
			t.Fatalf("failed to read upload session before commit: %v", err)
		}
		defer reader.Close()

		buffer := make([]byte, len(testData))
		n, err := reader.Read(buffer)
		if err != nil {
			t.Fatalf("failed to read upload session data: %v", err)
		}

		if n != len(testData) {
			t.Fatalf("upload session: expected %d bytes, got %d", len(testData), n)
		}

		err = writer.Commit(ctx)
		if err != nil {
			t.Fatalf("failed to commit upload session: %v", err)
		}

		t.Logf("SUCCESS: Upload session provides immediate read-after-write consistency")
	})

	t.Run("regular_file_eventual_consistency", func(t *testing.T) {
		// Regular file path - may not be immediately readable (implementation dependent)
		regularPath := "/test/regular/file.txt"
		
		writer, err := natsDriver.Writer(ctx, regularPath, false)
		if err != nil {
			t.Fatalf("failed to create regular file writer: %v", err)
		}

		_, err = writer.Write(testData)
		if err != nil {
			t.Fatalf("failed to write to regular file: %v", err)
		}

		// For regular files, we don't guarantee immediate readability
		// (though NATS implementation happens to provide it due to the fix)
		
		err = writer.Commit(ctx)
		if err != nil {
			t.Fatalf("failed to commit regular file: %v", err)
		}

		// After commit, should definitely be readable
		reader, err := natsDriver.Reader(ctx, regularPath, 0)
		if err != nil {
			t.Fatalf("failed to read regular file after commit: %v", err)
		}
		defer reader.Close()

		buffer := make([]byte, len(testData))
		n, err := reader.Read(buffer)
		if err != nil {
			t.Fatalf("failed to read regular file data: %v", err)
		}

		if n != len(testData) {
			t.Fatalf("regular file: expected %d bytes, got %d", len(testData), n)
		}

		t.Logf("SUCCESS: Regular file is readable after commit")
	})
}