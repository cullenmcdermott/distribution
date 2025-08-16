# NATS Storage Driver Troubleshooting

## Common Issues

### 1. "Path not found" Errors
**Symptoms**: HTTP 500 errors during upload completion
**Cause**: Stale Docker containers or incomplete rebuilds
**Solution**: 
```bash
docker-compose down
docker-compose up --build -d
```

### 2. Context Cancellation
**Symptoms**: Empty digest validation, "context canceled" errors
**Cause**: Request context being cancelled during chunk reads
**Solution**: The driver now uses background context for chunk operations

### 3. Race Conditions in Upload Sessions
**Symptoms**: Inconsistent chunk counts, metadata errors
**Cause**: Concurrent metadata updates during PATCH/PUT operations
**Solution**: Implemented double-counting prevention in strategies

## Testing Commands

```bash
# Build and test
make clean && make binaries
docker-compose up --build -d

# Test operations
docker pull hello-world
docker tag hello-world localhost:5002/hello-world:test
docker push localhost:5002/hello-world:test
docker pull localhost:5002/hello-world:test
```

## Debug Mode

Enable debug logging in config:
```yaml
log:
  level: debug
```

## Health Checks

The driver implements health checks via:
- NATS connection status
- KV store availability
- Basic connectivity tests