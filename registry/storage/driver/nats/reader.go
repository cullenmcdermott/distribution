package nats

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"time"
)

// chunkReader implements io.ReadSeekCloser for NATS chunk data
type chunkReader struct {
	driver     *natsDriver
	metadata   *StorageMetadata
	offset     int64
	position   int64
	pathHash   string                     // Hash for chunk keys
}

func newChunkReader(ctx context.Context, driver *natsDriver, metadata *StorageMetadata, offset int64) (*chunkReader, error) {
	// Determine pathHash based on metadata path
	pathInfo := driver.pathRouter.ParsePath(metadata.Path)
	var pathHash string
	if pathInfo.Type == PathTypeUploadData {
		pathHash = pathInfo.UploadID
	} else {
		pathHash = pathInfo.Digest
	}
	
	reader := &chunkReader{
		driver:     driver,
		metadata:   metadata,
		offset:     offset,
		position:   0,
		pathHash:   pathHash,
	}
	
	return reader, nil
}

// chunkKey creates an ObjectStore key for a chunk
func chunkKey(pathHash string, chunkIndex int) string {
	return fmt.Sprintf("chunks/%s/%d", pathHash, chunkIndex)
}

// getChunkMetadata gets chunk metadata from embedded chunks
func (r *chunkReader) getChunkMetadata(chunkIndex int) (*ChunkMetadata, error) {
	return r.metadata.GetChunk(chunkIndex)
}

func (r *chunkReader) Read(p []byte) (int, error) {
	// Calculate the current absolute position in the file
	absolutePos := r.offset + r.position
	
	// Check if we've reached the end of the file
	if absolutePos >= r.metadata.TotalSize {
		return 0, io.EOF
	}
	
	// Calculate how much data is still available
	remaining := r.metadata.TotalSize - absolutePos
	if int64(len(p)) > remaining {
		p = p[:remaining]
	}
	
	totalRead := 0
	
	// Read data chunk by chunk
	for totalRead < len(p) {
		// Find which chunk contains the current position
		chunkIndex, offsetInChunk, err := r.findChunkForPosition(absolutePos + int64(totalRead))
		if err != nil {
			return totalRead, err
		}
		
		// Check if chunk metadata exists in our embedded metadata
		_, chunkErr := r.getChunkMetadata(chunkIndex)
		if chunkErr != nil {
			// If chunk metadata doesn't exist, we've reached the end of available data
			break
		}
		
		// Additional safety check against metadata.ChunkCount for bounds checking
		if chunkIndex >= r.metadata.ChunkCount {
			// This could happen during race conditions - use the actual chunk existence as truth
			break
		}
		
		// Get the chunk data
		data, err := r.readChunk(chunkIndex)
		if err != nil {
			return totalRead, err
		}
		
		// Copy the relevant portion to the output buffer
		startPos := offsetInChunk
		endPos := int64(len(data))
		maxCopy := int64(len(p) - totalRead)
		
		if endPos-startPos > maxCopy {
			endPos = startPos + maxCopy
		}
		
		if startPos < endPos {
			copied := copy(p[totalRead:], data[startPos:endPos])
			totalRead += copied
		}
		
		// If we've copied less than available in this chunk, we're done
		if endPos < int64(len(data)) {
			break
		}
	}
	
	r.position += int64(totalRead)
	
	if totalRead == 0 && len(p) > 0 {
		return 0, io.EOF
	}
	
	return totalRead, nil
}

// findChunkForPosition returns the chunk index and offset within that chunk for a given absolute position
func (r *chunkReader) findChunkForPosition(absolutePos int64) (int, int64, error) {
	var currentPos int64 = 0
	
	// Scan chunks until we find the position OR run out of chunks
	for i := 0; i < r.metadata.ChunkCount; i++ {
		chunkMeta, err := r.getChunkMetadata(i)
		if err != nil {
			// If we can't load chunk metadata, we've reached the end
			return i, 0, nil
		}
		
		if absolutePos < currentPos+chunkMeta.Size {
			return i, absolutePos - currentPos, nil
		}
		currentPos += chunkMeta.Size
	}
	
	// Position is beyond all available chunks
	return r.metadata.ChunkCount, 0, nil
}

// readChunk reads the complete data from a specific chunk
func (r *chunkReader) readChunk(chunkIndex int) ([]byte, error) {
	chunkPath := chunkKey(r.pathHash, chunkIndex)
	
	// Use a timeout context for chunk reads to avoid hanging
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	
	chunkReader, err := r.driver.os.Get(ctx, chunkPath)
	if err != nil {
		return nil, parseNATSError(r.metadata.Path, err)
	}
	defer chunkReader.Close()
	
	data, err := io.ReadAll(chunkReader)
	if err != nil {
		return nil, parseNATSError(r.metadata.Path, err)
	}
	
	// Verify checksum if chunk metadata is available
	if chunkMeta, err := r.metadata.GetChunk(chunkIndex); err == nil && chunkMeta.Checksum != "" {
		checksum := fmt.Sprintf("%x", sha256.Sum256(data))
		if checksum != chunkMeta.Checksum {
			return nil, fmt.Errorf("chunk %d checksum mismatch: expected %s, got %s", 
				chunkIndex, chunkMeta.Checksum, checksum)
		}
	}
	
	return data, nil
}

func (r *chunkReader) Close() error {
	return nil
}