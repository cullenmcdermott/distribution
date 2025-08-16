# NATS Storage Driver Configuration

## Basic Configuration

```yaml
storage:
  nats:
    serverurl: "nats://localhost:4222"
    bucket: "registry-storage"
```

## Authentication Examples

### NKey Authentication

```yaml
storage:
  nats:
    serverurl: "nats://localhost:4222"
    bucket: "registry-storage"
    auth:
      type: "nkey"
      nkeyfile: "/path/to/nkey.seed"
      # OR use seed directly:
      # nkeyseed: "SUABC123..."
```

### User/Password Authentication

```yaml
storage:
  nats:
    serverurl: "nats://localhost:4222"
    bucket: "registry-storage"
    auth:
      type: "userpass"
      username: "registry_user"
      password: "secure_password"
```

### Token Authentication

```yaml
storage:
  nats:
    serverurl: "nats://localhost:4222"
    bucket: "registry-storage"
    auth:
      type: "token"
      token: "your_auth_token"
```

### JWT Authentication

```yaml
storage:
  nats:
    serverurl: "nats://localhost:4222"
    bucket: "registry-storage"
    auth:
      type: "jwt"
      jwt: "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..."
      nkeyseed: "SUABC123..."  # For signing
```

### Credentials File

```yaml
storage:
  nats:
    serverurl: "nats://localhost:4222"
    bucket: "registry-storage"
    auth:
      credentialsfile: "/path/to/creds.file"
```

## TLS Configuration

### Basic TLS

```yaml
storage:
  nats:
    serverurl: "tls://localhost:4222"
    bucket: "registry-storage"
    tls:
      enabled: true
```

### TLS with Client Certificates

```yaml
storage:
  nats:
    serverurl: "tls://localhost:4222"
    bucket: "registry-storage"
    tls:
      enabled: true
      certfile: "/path/to/client.crt"
      keyfile: "/path/to/client.key"
      cafile: "/path/to/ca.crt"
      servername: "nats.example.com"
```

### TLS with Custom CA

```yaml
storage:
  nats:
    serverurl: "tls://localhost:4222"
    bucket: "registry-storage"
    tls:
      enabled: true
      cafile: "/path/to/ca.crt"
      insecureskip: false  # Verify certificates
```

## Performance and Advanced Configuration

### Performance Tuning

```yaml
storage:
  nats:
    serverurl: "nats://localhost:4222"
    bucket: "registry-storage"
    performance:
      chunksize: 33554432      # 32MB chunks
      maxconnections: 10       # Max concurrent connections
      buffersize: 65536       # 64KB buffer
      compressionlevel: 0     # Disabled (0-9)
    timeouts:
      connect: "30s"
      requesttimeout: "60s"
      reconnectwait: "2s"
      maxreconnect: -1        # Unlimited reconnects
```

### JetStream Configuration

```yaml
storage:
  nats:
    serverurl: "nats://localhost:4222"
    bucket: "registry-storage"
    jetstream:
      domain: "hub"           # JetStream domain
      maxwait: "30s"         # Max wait for requests
      publishtimeout: "30s"   # Publish timeout
      ackwait: "30s"         # Acknowledgment wait
      maxackpending: 1000    # Max pending acks
      replicacount: 3        # Number of replicas
      storagetype: "file"    # "file" or "memory"
      retentionpolicy: "limits" # "limits", "interest", "workqueue"
```

## Complete Production Example

```yaml
version: 0.1
log:
  accesslog:
    disabled: false
  level: info
  formatter: json
storage:
  nats:
    serverurl: "tls://nats.company.com:4222"
    bucket: "prod-registry-storage"
    auth:
      type: "nkey"
      nkeyfile: "/etc/registry/nats.seed"
    tls:
      enabled: true
      certfile: "/etc/ssl/certs/registry-client.crt"
      keyfile: "/etc/ssl/private/registry-client.key"
      cafile: "/etc/ssl/certs/ca.crt"
      servername: "nats.company.com"
    performance:
      chunksize: 67108864    # 64MB for large images
      maxconnections: 20     # Higher concurrency
      buffersize: 131072     # 128KB buffer
    timeouts:
      connect: "10s"
      requesttimeout: "120s" # Longer for large uploads
      reconnectwait: "5s"
      maxreconnect: 10
    jetstream:
      maxwait: "60s"
      publishtimeout: "60s"
      ackwait: "60s"
      maxackpending: 2000
      replicacount: 3        # High availability
      storagetype: "file"
      retentionpolicy: "limits"
http:
  addr: :5000
  debug:
    addr: :5001
    prometheus:
      enabled: true
      path: /metrics
  relativeurls: false
  headers:
    X-Content-Type-Options: [nosniff]
health:
  storagedriver:
    enabled: true
    interval: 10s
    threshold: 3
```

## Docker Compose Setup

```yaml
services:
  nats:
    image: nats:latest
    command: ["--jetstream"]
    ports:
      - "4222:4222"
      - "8222:8222"
    
  registry:
    build: .
    ports:
      - "5002:5002"
      - "5003:5003"
    volumes:
      - "./config-nats.yml:/etc/distribution/config.yml"
    depends_on:
      - nats
```

## Environment Variables

- `NATS_SERVER_URL`: Override server URL
- `NATS_BUCKET`: Override bucket name