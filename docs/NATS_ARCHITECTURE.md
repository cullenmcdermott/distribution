# NATS Storage Driver Architecture

## Core Components

### 1. Path Router (`PathRouter`)
- Classifies registry paths into types (upload data, hashstates, regular)
- Uses regex patterns for efficient path parsing
- Enables clean separation of upload session logic
- Fixed SHA256 digest extraction (64-character handling)

### 2. Writer Strategies (`WriterStrategy`)
- **UploadSessionStrategy**: Immediate metadata updates, auto-commit on close
- **RegularFileStrategy**: Traditional commit behavior
- **HashstatesStrategy**: Minimal implementation for digest state
- All strategies now use `MaxChunkSize` tracking for optimization

### 3. Scalable Metadata System
- **StorageMetadata**: Core file metadata (size, chunk count, timestamps)
- **ChunkMetadata**: Individual chunk metadata with integrity verification
- **On-demand Loading**: Chunk metadata loaded only when needed
- **Caching**: Recently accessed chunk metadata cached for performance

### 4. Chunked Storage with Integrity
- Large files split into manageable chunks
- Each chunk has individual metadata with SHA256 checksum
- Efficient offset-based reading with metadata caching
- Background context prevents cancellation issues
- Optional checksum verification during reads

### 5. Authentication System
- Multi-method authentication (NKey, user/pass, token, JWT)
- TLS support with client certificates and custom CAs
- Comprehensive configuration options for security

## Data Flow

```
HTTP Request → PathRouter → Strategy Selection → Chunk Operations → NATS JetStream
                                                        ↓
                                                Metadata Operations → NATS KV Store
```

## Storage Layout

```
JetStream Object Store:
├── chunks/{pathHash}/{index}           # Actual data chunks
└── KV Store:
    ├── storage-{path-key}              # Core file metadata
    ├── upload-session-{uuid}           # Upload session metadata
    └── chunk-meta-{pathHash}-{index}   # Individual chunk metadata
```

## Metadata Structure

### StorageMetadata
```go
type StorageMetadata struct {
    Path         string    // Original registry path
    TotalSize    int64     // Total size across all chunks
    ChunkCount   int       // Number of chunks
    MaxChunkSize int64     // Maximum chunk size for buffer allocation
    UploadID     string    // Upload session ID (if applicable)
    CreatedAt    time.Time
    UpdatedAt    time.Time
    Version      int       // Metadata format version
}
```

### ChunkMetadata
```go
type ChunkMetadata struct {
    Size      int64     // Size of this chunk in bytes
    Checksum  string    // SHA256 checksum of chunk data
    CreatedAt time.Time
}
```

## Key Design Decisions

### Scalability
- **Individual Chunk Metadata**: Avoids large arrays in main metadata
- **On-demand Loading**: Reduces memory usage for large files
- **Caching**: Balances performance with memory efficiency

### Integrity
- **SHA256 Checksums**: Every chunk has verified integrity
- **Optional Verification**: Checksums checked during reads when available
- **Error Detection**: Corrupted chunks detected and reported

### Performance
- **MaxChunkSize Tracking**: Enables optimal buffer allocation
- **Background Context**: Prevents request cancellation during chunk operations
- **Efficient Path Parsing**: Regex-based path classification

## Known Limitations

### Current Issues
1. **Digest Validation**: Registry fails to find blobs during manifest validation
2. **Path Resolution**: Potential issue with digest-to-path mapping
3. **Test Integration**: Full workflow tests currently failing

### Future Improvements
1. **Metrics**: Add observability and monitoring
2. **Compression**: Optional chunk compression
3. **Replication**: Multi-region chunk replication
4. **Garbage Collection**: Automatic cleanup of orphaned chunks