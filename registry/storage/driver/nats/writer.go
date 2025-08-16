package nats

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"time"

	storagedriver "github.com/distribution/distribution/v3/registry/storage/driver"
	"github.com/nats-io/nats.go/jetstream"
)

type fileWriter struct {
	driver         *natsDriver
	metadata       *StorageMetadata
	chunkIndex     int
	chunkPath      string
	pipeWriter     *io.PipeWriter
	pipeReader     *io.PipeReader
	result         chan error
	size           int64
	originalSize   int64  // Track original metadata size to detect new data
	closed         bool
	chunkData      []byte // Buffer to track chunk data for checksum calculation
	pathHash       string // Hash for chunk metadata keys
	isHashstates   bool   // True if this is a hashstates writer
	hashstatesBuffer []byte // Buffer for hashstates data
}

func newFileWriter(ctx context.Context, driver *natsDriver, path string, append bool) (*fileWriter, error) {
	// Check path type
	pathInfo := driver.pathRouter.ParsePath(path)
	
	// Regular files don't support append, but upload session data paths do
	if append && pathInfo.Type != PathTypeUploadData {
		return nil, storagedriver.ErrUnsupportedMethod{DriverName: "nats"}
	}
	
	// Handle hashstates paths differently
	if pathInfo.Type == PathTypeUploadHashstates {
		return newHashstatesWriter(ctx, driver, path, append)
	}
	
	// Get or create metadata for regular files and upload sessions
	metadata, err := driver.getOrCreateStorageMetadata(ctx, path, append)
	if err != nil {
		return nil, err
	}
	
	// For new upload session data paths, save metadata immediately only if it's truly new
	if pathInfo.Type == PathTypeUploadData && !append && metadata.TotalSize == 0 {
		// Set up upload session metadata properly only for new sessions
		if metadata.UploadID == "" {
			metadata.UploadID = pathInfo.UploadID
		}
		err = driver.saveMetadata(ctx, metadata)
		if err != nil {
			return nil, fmt.Errorf("failed to save initial upload session metadata: %w", err)
		}
	}
	
	pr, pw := io.Pipe()
	chunkIndex := metadata.ChunkCount
	
	// Determine chunk path based on path type
	var chunkPath string
	var pathHash string
	if pathInfo.Type == PathTypeUploadData {
		pathHash = pathInfo.UploadID
		chunkPath = chunkKey(pathInfo.UploadID, chunkIndex)
	} else {
		pathHash = pathInfo.Digest
		chunkPath = chunkKey(pathInfo.Digest, chunkIndex)
	}
	
	w := &fileWriter{
		driver:       driver,
		metadata:     metadata,
		chunkIndex:   chunkIndex,
		chunkPath:    chunkPath,
		pipeWriter:   pw,
		pipeReader:   pr,
		result:       make(chan error, 1),
		size:         metadata.TotalSize, // Start with existing size for append
		originalSize: metadata.TotalSize, // Track original size for auto-commit detection
		chunkData:    make([]byte, 0),
		pathHash:     pathHash,
	}
	
	go w.streamChunkToNATS(ctx)
	
	return w, nil
}

func newHashstatesWriter(ctx context.Context, driver *natsDriver, path string, append bool) (*fileWriter, error) {
	// Create minimal metadata for hashstates
	metadata := &StorageMetadata{
		Path: path,
	}
	
	w := &fileWriter{
		driver:       driver,
		metadata:     metadata,
		isHashstates: true,
		hashstatesBuffer: make([]byte, 0),
		size:         0,
	}
	
	if append {
		// Try to get existing hashstates data
		existing, err := driver.getHashstatesData(ctx, path)
		if err == nil {
			w.hashstatesBuffer = existing
			w.size = int64(len(existing))
		}
	}
	
	return w, nil
}

func (w *fileWriter) streamChunkToNATS(ctx context.Context) {
	_, err := w.driver.os.Put(ctx, jetstream.ObjectMeta{Name: w.chunkPath}, w.pipeReader)
	w.result <- err
	w.pipeReader.Close()
}

func (w *fileWriter) Write(p []byte) (int, error) {
	if w.closed {
		return 0, errors.New("writer closed")
	}
	
	// Handle hashstates differently - they're buffered in memory
	if w.isHashstates {
		w.hashstatesBuffer = append(w.hashstatesBuffer, p...)
		n := len(p)
		w.size += int64(n)
		return n, nil
	}
	
	// For regular files and upload sessions, write to pipe
	n, err := w.pipeWriter.Write(p)
	w.size += int64(n)
	
	// Track chunk data for checksum calculation
	w.chunkData = append(w.chunkData, p[:n]...)
	
	// For upload sessions, make data immediately visible by committing chunks
	pathInfo := w.driver.pathRouter.ParsePath(w.metadata.Path)
	if pathInfo.Type == PathTypeUploadData {
		// Close current pipe to flush data to NATS
		w.pipeWriter.Close()
		
		// Wait for chunk to be written (no timeout - let caller handle cancellation)
		select {
		case writeErr := <-w.result:
			if writeErr != nil {
				return n, writeErr
			}
		}
		
		// Calculate chunk info and update metadata immediately
		chunkSize := w.size - w.originalSize
		if chunkSize > 0 {
			// Calculate checksum of chunk data
			checksum := sha256.Sum256(w.chunkData)
			checksumHex := hex.EncodeToString(checksum[:])
			
			// Add chunk to metadata with checksum
			w.metadata.AddChunk(chunkSize, checksumHex)
			
			// Use a timeout context for metadata save to avoid hanging
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			
			// Save metadata to make chunk immediately visible
			if err := w.driver.saveMetadata(ctx, w.metadata); err != nil {
				return n, fmt.Errorf("failed to save metadata after write: %w", err)
			}
		}
		
		// Prepare for next chunk if more writes come
		w.chunkIndex++
		w.originalSize = w.size
		w.chunkData = make([]byte, 0) // Reset chunk data buffer
		
		// Create new pipe for next chunk
		pr, pw := io.Pipe()
		w.pipeWriter = pw
		w.pipeReader = pr
		w.result = make(chan error, 1)
		w.chunkPath = chunkKey(w.pathHash, w.chunkIndex)
		
		// Start streaming the next chunk with timeout context
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		go func() {
			defer cancel()
			w.streamChunkToNATS(ctx)
		}()
	}
	
	return n, err
}

func (w *fileWriter) Size() int64 {
	return w.size
}

func (w *fileWriter) Close() error {
	if w.closed {
		return nil
	}
	w.closed = true
	
	// For hashstates, just mark as closed
	if w.isHashstates {
		return nil
	}
	
	// For regular files and upload sessions, close the pipe writer
	return w.pipeWriter.Close()
}

func (w *fileWriter) Cancel(ctx context.Context) error {
	if w.closed {
		return nil
	}
	w.closed = true
	
	if w.isHashstates {
		return nil
	}
	
	w.pipeWriter.CloseWithError(errors.New("cancelled"))
	<-w.result
	return nil
}

func (w *fileWriter) Commit(ctx context.Context) error {
	if err := w.Close(); err != nil {
		return err
	}
	
	// Handle hashstates commit
	if w.isHashstates {
		// Store hashstates data separately (does not affect blob size)
		return w.driver.putHashstatesData(ctx, w.metadata.Path, w.hashstatesBuffer)
	}
	
	// Check if this is an upload session that has already committed data during writes
	pathInfo := w.driver.pathRouter.ParsePath(w.metadata.Path)
	isUploadSession := pathInfo.Type == PathTypeUploadData
	
	// For upload sessions, data may already be committed in Write() calls
	// For regular files, commit all data now
	if !isUploadSession || w.size > w.originalSize {
		// Wait for any pending chunk to be written
		select {
		case err := <-w.result:
			if err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
		
		// Calculate remaining chunk info
		chunkSize := w.size - w.originalSize
		if chunkSize > 0 {
			// Calculate checksum of remaining chunk data
			checksum := sha256.Sum256(w.chunkData)
			checksumHex := hex.EncodeToString(checksum[:])
			
			// Add chunk to metadata with checksum
			w.metadata.AddChunk(chunkSize, checksumHex)
		}
		
		// Save metadata (for regular files this is the only save, for upload sessions this may be redundant but ensures consistency)
		return w.driver.saveMetadata(ctx, w.metadata)
	}
	
	// For upload sessions with no new data since last write, metadata is already saved
	return nil
}