# NATS Storage Driver Debugging Session - Status Report

## Project Overview

The NATS storage driver for Docker Distribution Registry has been developed and is functionally complete with a sophisticated chunked storage system. However, there's a critical issue preventing Docker push operations from working correctly in the production Docker Compose stack.

## Current Status: CRITICAL ISSUE IDENTIFIED BUT NOT RESOLVED

### ✅ What's Working
- **Integration Tests**: All NATS driver integration tests pass successfully
- **Core Architecture**: Chunked storage with scalable metadata system works correctly
- **Storage Operations**: Basic read/write operations work correctly when tested directly
- **Metadata System**: Individual chunk metadata with SHA256 integrity verification
- **Authentication**: Comprehensive auth system (NKey, user/pass, token, JWT, TLS)

### ❌ Critical Issue: Docker Push Digest Validation Failure

**Symptom**: Docker push operations fail with "digest invalid: provided digest did not match uploaded content"

**Root Cause Identified**: During Docker registry blob validation, the NATS driver returns empty data instead of the actual blob content, causing digest validation to fail.

**Error Pattern**:
```
canonical digest: sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855 (empty string hash)
provided digest: sha256:c9c5fd25a1bdc181cb012bc4fbb1ab272a975728f54064b7ae3ee8e77fd28c46 (expected blob hash)
```

## Troubleshooting Work Completed

### 1. Initial Issue Analysis ✅
- **Problem**: Integration tests were failing due to missing config blobs in manifests
- **Solution**: Fixed integration tests to actually upload config blobs before creating manifests
- **Result**: Integration tests now pass, but Docker Compose stack still fails

### 2. Registry Manifest Validation Analysis ✅
- **Investigation**: Understood how Docker registry validates manifests and blob existence
- **Finding**: Registry correctly requires all referenced blobs to exist before accepting manifests
- **Conclusion**: The registry validation logic is working correctly

### 3. NATS Driver Workflow Analysis ✅
- **Testing**: Created standalone tests to verify NATS driver functionality
- **Finding**: NATS driver works correctly in isolation for upload session workflows
- **Test Results**: 
  - ✅ Basic read/write operations work
  - ✅ Upload session simulation works
  - ✅ Chunked storage and reconstruction works

### 4. Docker Registry Upload Workflow Analysis ✅
- **Investigation**: Analyzed the Docker registry upload process:
  1. POST `/v2/{name}/blobs/uploads/` - creates upload session
  2. PATCH `/v2/{name}/blobs/uploads/{uuid}` - uploads blob data (3156 bytes)
  3. PUT `/v2/{name}/blobs/uploads/{uuid}?digest=...` - validates and commits
- **Finding**: Issue occurs during step 3 (validation), not during data upload

### 5. Storage Driver Integration Analysis ✅
- **Investigation**: Examined how Docker registry calls the storage driver during validation
- **Key Finding**: Registry reads blob data using `driver.Reader()` from upload session path for validation
- **Problem Identified**: Reader returns empty data during validation step

### 6. Metadata Corruption Issue Investigation ✅
- **Hypothesis**: Upload session metadata being overwritten during PUT validation
- **Root Cause Found**: Writer creation with `append=false` during PUT was saving fresh metadata with `ChunkCount=0`
- **Fix Attempted**: Modified writer creation logic to only save metadata for truly new sessions
- **Result**: Fix applied but issue persists

## Current Understanding of the Problem

### The Issue is NOT:
- ❌ Missing config blobs in manifests (fixed)
- ❌ Basic NATS driver functionality (works correctly)
- ❌ Integration test logic (passes)
- ❌ Metadata corruption during writer creation (attempted fix didn't resolve)

### The Issue IS:
- ✅ NATS driver Reader returning empty data during Docker registry validation
- ✅ Occurs specifically in production Docker workflow, not in direct testing
- ✅ Happens during PUT digest validation step, after successful PATCH upload

## Technical Architecture Context

### NATS Driver Architecture
```
Upload Session Path: /docker/registry/v2/repositories/{repo}/_uploads/{uuid}/data
Storage: Chunked in NATS JetStream Object Store
Metadata: Stored in NATS KV store with individual chunk metadata
```

### Upload Workflow
1. **POST**: Creates upload session metadata
2. **PATCH**: Writes blob data in chunks, updates metadata (`append=true`)
3. **PUT**: Registry validates by reading data back (`append=false`, no new data)

### Chunk Storage System
- Data split into chunks stored in ObjectStore: `chunks/{pathHash}/{chunkIndex}`
- Metadata stored in KV: `storage-{sanitized-path}` and `chunk-meta-{pathHash}-{chunkIndex}`
- Reader reconstructs data from chunks using metadata

## Debugging Evidence

### Registry Logs Show:
```
time="2025-07-18T18:10:48.520202313Z" level=error msg="canonical digest does match provided digest" 
canonical="sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855" 
provided="sha256:c9c5fd25a1bdc181cb012bc4fbb1ab272a975728f54064b7ae3ee8e77fd28c46"
```

### NATS Storage Shows:
- 2 streams created
- 37 messages stored
- Data is being persisted to NATS

## Next Steps for Resolution

### Priority 1: Debug Reader During Validation
The core issue is that `driver.Reader()` returns empty data during Docker registry validation. Need to:

1. **Add Debug Logging**: Add temporary debug output to `driver.Reader()` to see:
   - What path is being requested during validation
   - What metadata is loaded
   - What chunk data is found
   - Whether chunks are being read correctly

2. **Trace Validation Call**: Understand exactly how the registry calls the Reader during PUT validation:
   - Is it using the correct upload session path?
   - Is there a timing issue?
   - Are there multiple concurrent Reader calls interfering?

### Priority 2: Investigate Path/Metadata Issues
Check for subtle path or metadata handling issues:

1. **Path Consistency**: Verify the exact path used during PATCH vs PUT
2. **Upload ID Consistency**: Ensure upload ID is correctly extracted and used
3. **Metadata Keys**: Verify metadata is stored and retrieved with correct keys

### Priority 3: Check for Race Conditions
Investigate potential timing/concurrency issues:

1. **Commit Timing**: Ensure PATCH commit completes before PUT validation
2. **Metadata Consistency**: Check if metadata is immediately available after PATCH
3. **Context Cancellation**: Verify background context usage is correct

## Key Files Modified

### Fixed Files:
- `/registry/storage/driver/nats/integration_test.go` - Fixed config blob upload issue
- `/registry/storage/driver/nats/driver.go` - Removed debug statements
- `/registry/storage/driver/nats/upload_session.go` - Cleaned up debug code
- `/registry/storage/driver/nats/writer.go` - Attempted metadata corruption fix

### Core Implementation Files:
- `/registry/storage/driver/nats/driver.go` - Main driver with Reader/Writer methods
- `/registry/storage/driver/nats/storage.go` - Metadata management system
- `/registry/storage/driver/nats/reader.go` - Chunk reconstruction logic
- `/registry/storage/driver/nats/writer.go` - Chunked writing with strategies

## Test Commands

### Verify Integration Tests:
```bash
go test -v ./registry/storage/driver/nats/... -run TestNATSIntegration
```

### Test Docker Compose Stack:
```bash
docker-compose down && docker-compose up --build -d
docker push localhost:5002/hello-world:test
```

### Check Registry Logs:
```bash
docker logs distribution-registry-1 | grep -E "(canonical|digest.*match)"
```

## LLM Handoff Prompt

Use this prompt to continue debugging from where this session left off:

---

**Context**: I'm working on a NATS storage driver for Docker Distribution Registry. The driver has a sophisticated chunked storage system and passes all integration tests, but Docker push operations fail during digest validation in the production Docker Compose stack.

**Critical Issue**: During Docker registry blob upload validation (PUT step), the NATS driver's Reader method returns empty data instead of the actual blob content, causing digest validation to fail with "canonical digest does not match provided digest" error.

**Current State**: 
- Integration tests pass ✅
- Standalone NATS driver tests work ✅
- Docker Compose stack fails with digest validation error ❌
- Issue isolated to Reader method during PUT validation step
- Attempted fix for metadata corruption didn't resolve the issue

**Immediate Need**: Debug why `driver.Reader()` returns empty data during Docker registry validation in the upload session workflow. The Reader should reconstruct blob data from chunks stored in NATS JetStream.

**Environment**: 
- Docker Compose stack running NATS + Distribution registry
- Registry accessible at localhost:5002
- NATS at localhost:4222
- Working directory: `/Users/cullen/git/distribution`

**Previous Debugging**: Extensive analysis completed (see NATS_DEBUGGING_SESSION.md for full details). Ready to add debug logging to Reader method and trace the validation call path.

**Next Steps**: Add debug output to understand what happens when Docker registry calls `driver.Reader()` during PUT validation, specifically checking path handling, metadata loading, and chunk reconstruction.

Please continue debugging from this point, focusing on the Reader method during Docker push validation.

---

## Conclusion

The NATS storage driver is architecturally sound and functionally correct for most operations. The issue is specifically in how the Reader reconstructs blob data during Docker registry validation. The next debugging session should focus on adding targeted debug output to trace the exact failure point in the Reader method during the PUT validation step.