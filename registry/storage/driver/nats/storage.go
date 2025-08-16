package nats

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// StorageMetadata represents unified metadata for all storage operations
// This consolidates the previous ObjectMetadata and UploadSessionMetadata structures
type StorageMetadata struct {
	// Core object information
	Path         string          `json:"path"`           // Original registry path
	TotalSize    int64           `json:"total_size"`     // Total size across all chunks
	ChunkCount   int             `json:"chunk_count"`    // Number of chunks
	MaxChunkSize int64           `json:"max_chunk_size"` // Maximum chunk size for buffer allocation optimization
	Chunks       []ChunkMetadata `json:"chunks"`         // Embedded chunk metadata

	// Upload session information (optional - only for upload sessions)
	UploadID   string `json:"upload_id,omitempty"`   // UUID from upload session
	Repository string `json:"repository,omitempty"`  // Repository name
	ObjectName string `json:"object_name,omitempty"` // NATS ObjectStore object name

	// Timestamps
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`

	// Format version for backward compatibility
	Version int `json:"version"`
}

// MetadataVersion represents the current version of the metadata format
const MetadataVersion = 3

// ChunkMetadata represents metadata for an individual chunk
type ChunkMetadata struct {
	Size      int64     `json:"size"`     // Size of this chunk in bytes
	Checksum  string    `json:"checksum"` // SHA256 checksum of chunk data
	CreatedAt time.Time `json:"created_at"`
}

// NewChunkMetadata creates a new ChunkMetadata instance
func NewChunkMetadata(size int64, checksum string) *ChunkMetadata {
	return &ChunkMetadata{
		Size:      size,
		Checksum:  checksum,
		CreatedAt: time.Now(),
	}
}

// metadataKey creates a KV key for storage metadata
// NATS KV keys have restrictions, so we need to sanitize the path
func metadataKey(path string) string {
	// Replace problematic characters with safe alternatives
	// / -> _
	// : -> -
	safeKey := strings.ReplaceAll(path, "/", "_")
	safeKey = strings.ReplaceAll(safeKey, ":", "-")
	return fmt.Sprintf("storage-%s", safeKey)
}

// chunkMetadataKey creates a KV key for individual chunk metadata (legacy - for migration only)
func chunkMetadataKey(pathHash string, chunkIndex int) string {
	return fmt.Sprintf("chunk-meta-%s-%d", pathHash, chunkIndex)
}

// NewStorageMetadata creates a new StorageMetadata instance
func NewStorageMetadata(path string) *StorageMetadata {
	now := time.Now()
	return &StorageMetadata{
		Path:         path,
		TotalSize:    0,
		ChunkCount:   0,
		MaxChunkSize: 0,
		Chunks:       make([]ChunkMetadata, 0),
		CreatedAt:    now,
		UpdatedAt:    now,
		Version:      MetadataVersion,
	}
}

// NewUploadSessionMetadata creates a new StorageMetadata for upload sessions
func NewUploadSessionMetadata(path, uploadID, repository string) *StorageMetadata {
	now := time.Now()
	return &StorageMetadata{
		Path:         path,
		TotalSize:    0,
		ChunkCount:   0,
		MaxChunkSize: 0,
		Chunks:       make([]ChunkMetadata, 0),
		UploadID:     uploadID,
		Repository:   repository,
		ObjectName:   fmt.Sprintf("upload-%s", uploadID), // Default object name
		CreatedAt:    now,
		UpdatedAt:    now,
		Version:      MetadataVersion,
	}
}

// IsUploadSession returns true if this metadata represents an upload session
func (sm *StorageMetadata) IsUploadSession() bool {
	return sm.UploadID != ""
}

// Touch updates the UpdatedAt timestamp
func (sm *StorageMetadata) Touch() {
	sm.UpdatedAt = time.Now()
}

// AddChunk adds a chunk to the metadata with checksum
func (sm *StorageMetadata) AddChunk(size int64, checksum string) {
	chunk := ChunkMetadata{
		Size:      size,
		Checksum:  checksum,
		CreatedAt: time.Now(),
	}
	sm.Chunks = append(sm.Chunks, chunk)
	sm.ChunkCount = len(sm.Chunks)
	sm.TotalSize += size
	if size > sm.MaxChunkSize {
		sm.MaxChunkSize = size
	}
	sm.Touch()
}

// GetChunk returns chunk metadata by index
func (sm *StorageMetadata) GetChunk(index int) (*ChunkMetadata, error) {
	if index < 0 || index >= len(sm.Chunks) {
		return nil, fmt.Errorf("chunk index %d out of range [0, %d)", index, len(sm.Chunks))
	}
	return &sm.Chunks[index], nil
}

// saveMetadata saves metadata to NATS KV store
func (d *natsDriver) saveMetadata(ctx context.Context, metadata *StorageMetadata) error {
	key := metadataKey(metadata.Path)

	// Update timestamp
	metadata.Touch()

	// Serialize metadata
	data, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	// Store in KV
	_, err = d.kv.Put(ctx, key, data)
	if err != nil {
		return fmt.Errorf("failed to save metadata to KV: %w", err)
	}

	return nil
}

// loadMetadata loads metadata from NATS KV store
func (d *natsDriver) loadMetadata(ctx context.Context, path string) (*StorageMetadata, error) {
	key := metadataKey(path)

	// Try to load from KV
	entry, err := d.kv.Get(ctx, key)
	if err != nil {
		if err == jetstream.ErrKeyNotFound {
			return nil, fmt.Errorf("metadata not found for path: %s", path)
		}
		return nil, fmt.Errorf("failed to get metadata from KV: %w", err)
	}

	// Deserialize metadata
	var metadata StorageMetadata
	if err := json.Unmarshal(entry.Value(), &metadata); err != nil {
		return nil, fmt.Errorf("failed to unmarshal metadata: %w", err)
	}

	// Handle backward compatibility for older metadata formats
	if metadata.Version == 0 {
		metadata.Version = MetadataVersion
	}

	// Migrate from separate chunk metadata to embedded chunks for older versions
	if metadata.Version < 3 && len(metadata.Chunks) == 0 && metadata.ChunkCount > 0 {
		pathInfo := d.pathRouter.ParsePath(path)
		var pathHash string
		if pathInfo.Type == PathTypeUploadData {
			pathHash = pathInfo.UploadID
		} else {
			pathHash = pathInfo.Digest
		}

		// Load chunk metadata from separate keys (best effort migration)
		for i := 0; i < metadata.ChunkCount; i++ {
			if chunkMeta, err := d.loadChunkMetadata(ctx, pathHash, i); err == nil {
				metadata.Chunks = append(metadata.Chunks, *chunkMeta)
			}
		}
		metadata.Version = MetadataVersion
	}

	// Initialize Chunks slice if nil (for newer metadata without chunks)
	if metadata.Chunks == nil {
		metadata.Chunks = make([]ChunkMetadata, 0)
	}

	return &metadata, nil
}

// deleteMetadata removes metadata from NATS KV store
func (d *natsDriver) deleteMetadata(ctx context.Context, path string) error {
	key := metadataKey(path)

	// Delete metadata key
	return d.kv.Delete(ctx, key)
}

// Legacy function for backward compatibility during migration
func (d *natsDriver) loadChunkMetadata(ctx context.Context, pathHash string, chunkIndex int) (*ChunkMetadata, error) {
	key := chunkMetadataKey(pathHash, chunkIndex)

	// Try to load from KV
	entry, err := d.kv.Get(ctx, key)
	if err != nil {
		if err == jetstream.ErrKeyNotFound {
			return nil, fmt.Errorf("chunk metadata not found: %s", key)
		}
		return nil, fmt.Errorf("failed to get chunk metadata from KV: %w", err)
	}

	// Deserialize metadata
	var chunkMeta ChunkMetadata
	if err := json.Unmarshal(entry.Value(), &chunkMeta); err != nil {
		return nil, fmt.Errorf("failed to unmarshal chunk metadata: %w", err)
	}

	return &chunkMeta, nil
}

// getOrCreateStorageMetadata retrieves existing metadata or creates new one
func (d *natsDriver) getOrCreateStorageMetadata(ctx context.Context, path string, append bool) (*StorageMetadata, error) {
	// Check if this is an upload session path
	pathInfo := d.pathRouter.ParsePath(path)
	if pathInfo.Type == PathTypeUploadData {
		// For upload sessions, ALWAYS try to load existing metadata first
		// This handles both append=true (PATCH) and append=false (PUT) cases
		metadata, err := d.loadMetadata(ctx, path)
		if err == nil {
			return metadata, nil
		}

		// If no existing metadata and we're appending, that's an error
		if append {
			return nil, fmt.Errorf("cannot append to non-existent upload session: %s", path)
		}

		// Create new upload session metadata only if none exists
		metadata = NewUploadSessionMetadata(path, pathInfo.UploadID, pathInfo.Repository)
		return metadata, nil
	}

	// For regular files, try to load existing metadata if appending
	if append {
		metadata, err := d.loadMetadata(ctx, path)
		if err == nil {
			return metadata, nil
		}
		// If metadata doesn't exist and we're appending, that's an error
		return nil, fmt.Errorf("cannot append to non-existent object: %s", path)
	}

	// Create new regular metadata
	metadata := NewStorageMetadata(path)
	return metadata, nil
}
