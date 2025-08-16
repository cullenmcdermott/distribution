package nats

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/distribution/distribution/v3/registry/storage/driver"
	"github.com/distribution/distribution/v3/registry/storage/driver/base"
	"github.com/distribution/distribution/v3/registry/storage/driver/factory"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nkeys"
)

const driverName = "nats"

type natsDriverFactory struct{}

func (factory *natsDriverFactory) Create(ctx context.Context, parameters map[string]interface{}) (driver.StorageDriver, error) {
	config, err := parseConfig(parameters)
	if err != nil {
		return nil, fmt.Errorf("NATS driver config validation failed: %w", err)
	}
	return New(ctx, config)
}

func init() {
	factory.Register(driverName, &natsDriverFactory{})
}

// PathType represents different types of registry paths
type PathType int

const (
	PathTypeRegular          PathType = iota  // 0
	PathTypeUploadData                        // 1  
	PathTypeUploadHashstates                  // 2
)

// PathInfo contains parsed information about a registry path
type PathInfo struct {
	Type        PathType
	UploadID    string
	Repository  string
	Digest      string
}

// PathRouter consolidates all path parsing logic into a single component
type PathRouter struct {
	sha256PathRegex *regexp.Regexp
	uploadDataRegex *regexp.Regexp
	uploadHashRegex *regexp.Regexp
}

// NewPathRouter creates a new PathRouter with compiled regex patterns
func NewPathRouter() *PathRouter {
	return &PathRouter{
		// Match SHA256 paths like /sha256/ab/abc123...def (captures 2-char prefix and remaining 62 chars)
		sha256PathRegex: regexp.MustCompile(`/sha256/([a-f0-9]{2})/([a-f0-9]{62})`),
		// Match upload data paths like /docker/registry/v2/repositories/{repo}/_uploads/{uuid}/data
		uploadDataRegex: regexp.MustCompile(`/docker/registry/v2/repositories/([^/]+(?:/[^/]+)*?)/_uploads/([^/]+)/data$`),
		// Match upload hashstates paths like /docker/registry/v2/repositories/{repo}/_uploads/{uuid}/hashstates/
		uploadHashRegex: regexp.MustCompile(`/docker/registry/v2/repositories/([^/]+(?:/[^/]+)*?)/_uploads/([^/]+)/hashstates/`),
	}
}

// ParsePath analyzes a registry path and returns structured information about it
func (pr *PathRouter) ParsePath(path string) PathInfo {
	// Check for upload data paths first (most specific)
	if matches := pr.uploadDataRegex.FindStringSubmatch(path); len(matches) == 3 {
		return PathInfo{
			Type:       PathTypeUploadData,
			Repository: matches[1],
			UploadID:   matches[2],
		}
	}
	
	// Check for upload hashstates paths
	if matches := pr.uploadHashRegex.FindStringSubmatch(path); len(matches) == 3 {
		return PathInfo{
			Type:       PathTypeUploadHashstates,
			Repository: matches[1],
			UploadID:   matches[2],
		}
	}
	
	// For regular paths, extract digest if available
	return PathInfo{
		Type:   PathTypeRegular,
		Digest: pr.extractDigest(path),
	}
}

// extractDigest safely extracts SHA256 digest from path using regex
func (pr *PathRouter) extractDigest(path string) string {
	if matches := pr.sha256PathRegex.FindStringSubmatch(path); len(matches) == 3 {
		// Reconstruct full 64-character digest from 2-char prefix + 62-char suffix
		return matches[1] + matches[2]
	}
	
	// Fallback to full hash (no truncation to avoid collisions)
	h := sha256.Sum256([]byte(path))
	return hex.EncodeToString(h[:])
}

type natsDriver struct {
	base.Base

	nc         *nats.Conn
	js         jetstream.JetStream
	os         jetstream.ObjectStore
	kv         jetstream.KeyValue
	pathRouter *PathRouter

	config Config
}

type baseEmbed struct {
	base.Base
}

type Driver struct {
	baseEmbed
}

func New(ctx context.Context, config Config) (driver.StorageDriver, error) {
	// Build NATS connection options
	opts := []nats.Option{
		nats.Name("registry-nats-driver"),
		nats.MaxReconnects(config.Timeouts.MaxReconnect),
		nats.ReconnectWait(config.Timeouts.ReconnectWait),
		nats.Timeout(config.Timeouts.Connect),
	}

	// Configure authentication
	authOpts, err := buildAuthOptions(config.Auth)
	if err != nil {
		return nil, fmt.Errorf("failed to configure authentication: %w", err)
	}
	opts = append(opts, authOpts...)

	// Configure TLS
	if config.TLS.Enabled {
		tlsConfig, err := buildTLSConfig(config.TLS)
		if err != nil {
			return nil, fmt.Errorf("failed to configure TLS: %w", err)
		}
		opts = append(opts, nats.Secure(tlsConfig))
	}

	// Connect to NATS
	nc, err := nats.Connect(config.ServerURL, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to NATS server at %s: %w", config.ServerURL, err)
	}

	// Create JetStream context with configuration
	jsOpts := []jetstream.JetStreamOpt{}
	if config.JetStream.Domain != "" {
		// Domain configuration would be handled in NATS connection setup
	}
	if config.JetStream.APIPrefix != "" {
		// API prefix configuration would be handled in NATS connection setup
	}

	js, err := jetstream.New(nc, jsOpts...)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("failed to create JetStream context: %w", err)
	}

	// Create or update object store with advanced configuration
	osConfig := jetstream.ObjectStoreConfig{
		Bucket:      config.Bucket,
		Description: "Docker Registry Storage",
		MaxBytes:    -1, // Unlimited
		Storage:     jetstream.FileStorage,
		Replicas:    config.JetStream.ReplicaCount,
		Placement:   &jetstream.Placement{},
		TTL:         0, // No TTL
		Compression: false,
	}

	// Set storage type
	if config.JetStream.StorageType == "memory" {
		osConfig.Storage = jetstream.MemoryStorage
	}

	os, err := js.CreateOrUpdateObjectStore(ctx, osConfig)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("failed to create or get object store bucket '%s': %w", config.Bucket, err)
	}

	// Create or update key-value store for metadata
	kvConfig := jetstream.KeyValueConfig{
		Bucket:      config.Bucket + "_metadata",
		Description: "Docker Registry Metadata",
		MaxBytes:    -1, // Unlimited
		Storage:     jetstream.FileStorage,
		Replicas:    config.JetStream.ReplicaCount,
		TTL:         0, // No TTL
		History:     1, // Keep only latest version
		Compression: false,
	}

	// Set storage type
	if config.JetStream.StorageType == "memory" {
		kvConfig.Storage = jetstream.MemoryStorage
	}

	kv, err := js.CreateOrUpdateKeyValue(ctx, kvConfig)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("failed to create or get key-value store bucket '%s': %w", kvConfig.Bucket, err)
	}

	d := &natsDriver{
		nc:         nc,
		js:         js,
		os:         os,
		kv:         kv,
		pathRouter: NewPathRouter(),
		config:     config,
	}

	return d, nil
}

func (d *natsDriver) Name() string {
	return driverName
}

func (d *natsDriver) GetContent(ctx context.Context, path string) ([]byte, error) {
	// Use the Reader method which handles chunked objects
	reader, err := d.Reader(ctx, path, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to create reader for path '%s': %w", path, err)
	}
	defer reader.Close()
	
	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to read content from path '%s': %w", path, err)
	}
	
	return data, nil
}

func (d *natsDriver) PutContent(ctx context.Context, path string, content []byte) error {
	writer, err := d.Writer(ctx, path, false)
	if err != nil {
		return fmt.Errorf("failed to create writer for path '%s': %w", path, err)
	}
	
	_, err = writer.Write(content)
	if err != nil {
		writer.Cancel(ctx)
		return fmt.Errorf("failed to write %d bytes to path '%s': %w", len(content), path, err)
	}
	
	err = writer.Commit(ctx)
	if err != nil {
		return fmt.Errorf("failed to commit write to path '%s': %w", path, err)
	}
	
	return nil
}

func (d *natsDriver) Reader(ctx context.Context, path string, offset int64) (io.ReadCloser, error) {
	// Use PathRouter for clean path classification
	pathInfo := d.pathRouter.ParsePath(path)
	
	switch pathInfo.Type {
	case PathTypeUploadData:
		// Handle actual blob data for upload sessions
		metadata, err := d.loadMetadata(ctx, path)
		if err != nil {
			return nil, parseNATSError(path, err)
		}
		
		// CRITICAL FIX: Don't rely solely on metadata.ChunkCount due to race conditions
		// During upload sessions, TotalSize may be updated before ChunkCount
		// Let the chunkReader handle the actual chunk existence checking
		if metadata.TotalSize == 0 && metadata.ChunkCount == 0 {
			return nil, driver.PathNotFoundError{Path: path}
		}
		
		return newChunkReader(ctx, d, metadata, offset)
		
	case PathTypeUploadHashstates:
		data, err := d.getHashstatesData(ctx, path)
		if err != nil {
			return nil, parseNATSError(path, err)
		}
		
		// Handle offset for hashstates
		if offset > int64(len(data)) {
			return nil, driver.InvalidOffsetError{Path: path, Offset: offset}
		}
		
		return io.NopCloser(bytes.NewReader(data[offset:])), nil
		
	default:
		// Regular file paths - use unified metadata
		metadata, err := d.loadMetadata(ctx, path)
		if err != nil {
			return nil, parseNATSError(path, err)
		}
		
		// For regular files, we can be stricter about ChunkCount since there's no race condition
		if metadata.ChunkCount == 0 {
			return nil, driver.PathNotFoundError{Path: path}
		}
		
		return newChunkReader(ctx, d, metadata, offset)
	}
}

func (d *natsDriver) Writer(ctx context.Context, path string, append bool) (driver.FileWriter, error) {
	// Unified writer handles all path types internally
	return newFileWriter(ctx, d, path, append)
}

func (d *natsDriver) Stat(ctx context.Context, path string) (driver.FileInfo, error) {
	// Handle root path for health checks - registry expects this to fail with PathNotFoundError
	if path == "/" {
		return nil, driver.PathNotFoundError{Path: path}
	}
	
	// Use PathRouter for clean path classification
	pathInfo := d.pathRouter.ParsePath(path)
	
	switch pathInfo.Type {
	case PathTypeUploadData:
		// Use unified metadata system for upload session data
		metadata, err := d.loadMetadata(ctx, path)
		if err != nil {
			return nil, parseNATSError(path, err)
		}
		
		return driver.FileInfoInternal{
			FileInfoFields: driver.FileInfoFields{
				Path:    path,
				Size:    metadata.TotalSize, // Use chunked storage total size
				ModTime: metadata.UpdatedAt,
				IsDir:   false,
			},
		}, nil
		
	case PathTypeUploadHashstates:
		data, err := d.getHashstatesData(ctx, path)
		if err != nil {
			return nil, parseNATSError(path, err)
		}
		
		return driver.FileInfoInternal{
			FileInfoFields: driver.FileInfoFields{
				Path:    path,
				Size:    int64(len(data)),
				ModTime: time.Now(), // Hashstates don't have timestamps
				IsDir:   false,
			},
		}, nil
		
	default:
		// Regular file paths - use unified metadata system
		metadata, err := d.loadMetadata(ctx, path)
		if err != nil {
			return nil, parseNATSError(path, err)
		}
		
		return driver.FileInfoInternal{
			FileInfoFields: driver.FileInfoFields{
				Path:    path,
				Size:    metadata.TotalSize,
				ModTime: metadata.UpdatedAt,
				IsDir:   false,
			},
		}, nil
	}
}

func (d *natsDriver) List(ctx context.Context, path string) ([]string, error) {
	if path == "/" {
		path = ""
	}
	prefix := strings.TrimPrefix(path, "/")
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	objects, err := d.os.List(ctx)
	if err != nil {
		return nil, parseNATSError(path, err)
	}
	
	var results []string
	for _, obj := range objects {
		objPath := "/" + obj.Name
		if prefix == "" || strings.HasPrefix(obj.Name, prefix) {
			results = append(results, objPath)
		}
	}
	
	return results, nil
}

func (d *natsDriver) Move(ctx context.Context, sourcePath, destPath string) error {
	content, err := d.GetContent(ctx, sourcePath)
	if err != nil {
		return fmt.Errorf("failed to read source path '%s' during move: %w", sourcePath, err)
	}
	
	err = d.PutContent(ctx, destPath, content)
	if err != nil {
		return fmt.Errorf("failed to write to destination path '%s' during move: %w", destPath, err)
	}
	
	err = d.Delete(ctx, sourcePath)
	if err != nil {
		return fmt.Errorf("failed to delete source path '%s' after move to '%s': %w", sourcePath, destPath, err)
	}
	
	return nil
}

func (d *natsDriver) Delete(ctx context.Context, path string) error {
	// Get object metadata first using unified system
	metadata, err := d.loadMetadata(ctx, path)
	if err != nil {
		return parseNATSError(path, err)
	}
	
	// Delete all chunks
	pathInfo := d.pathRouter.ParsePath(path)
	var pathHash string
	if pathInfo.Type == PathTypeUploadData {
		pathHash = pathInfo.UploadID
	} else {
		pathHash = pathInfo.Digest
	}
	
	// Delete each chunk
	for i := 0; i < metadata.ChunkCount; i++ {
		// Delete chunk data
		chunkPath := chunkKey(pathHash, i)
		err := d.os.Delete(ctx, chunkPath)
		if err != nil && err != jetstream.ErrObjectNotFound {
			// Log error but continue with cleanup - could add proper logging here
		}
	}
	
	// Delete main metadata
	return d.deleteMetadata(ctx, path)
}

func (d *natsDriver) RedirectURL(r *http.Request, path string) (string, error) {
	return "", nil
}

func (d *natsDriver) Walk(ctx context.Context, path string, f driver.WalkFn, options ...func(*driver.WalkOptions)) error {
	return driver.WalkFallback(ctx, d, path, f)
}

// HealthCheck verifies the driver is healthy and can connect to NATS
func (d *natsDriver) HealthCheck() error {
	if d.nc == nil || !d.nc.IsConnected() {
		return fmt.Errorf("NATS connection is not available (url: %s)", d.config.ServerURL)
	}
	
	// Test KV store connectivity with a simple operation
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	
	// Test basic KV connectivity by checking if our KV store exists
	info, err := d.kv.Status(ctx)
	if err != nil {
		return fmt.Errorf("NATS KV health check failed for bucket '%s': %w", d.config.Bucket+"_metadata", err)
	}
	if info == nil {
		return fmt.Errorf("NATS KV store bucket '%s' not available", d.config.Bucket+"_metadata")
	}
	
	return nil
}

func (d *natsDriver) natsPath(registryPath string) string {
	return strings.TrimPrefix(registryPath, "/docker/registry/v2/")
}

func parseNATSError(path string, err error) error {
	if err == jetstream.ErrObjectNotFound {
		return driver.PathNotFoundError{Path: path, DriverName: driverName}
	}
	
	if err == jetstream.ErrKeyNotFound {
		return driver.PathNotFoundError{Path: path, DriverName: driverName}
	}
	
	// Handle NATS errors - check for "not found" in error message
	if strings.Contains(err.Error(), "not found") || strings.Contains(err.Error(), "key not found") {
		return driver.PathNotFoundError{Path: path, DriverName: driverName}
	}
	
	return driver.Error{
		DriverName: driverName,
		Detail:     fmt.Errorf("NATS operation failed for path '%s': %w", path, err),
	}
}

// putHashstatesData stores hashstates data separately from blob data
func (d *natsDriver) putHashstatesData(ctx context.Context, path string, data []byte) error {
	// Store hashstates data as a separate NATS object
	pathInfo := d.pathRouter.ParsePath(path)
	objectName := fmt.Sprintf("hashstates-%s", pathInfo.Digest)
	_, err := d.os.Put(ctx, jetstream.ObjectMeta{Name: objectName}, bytes.NewReader(data))
	return err
}

// getHashstatesData retrieves hashstates data
func (d *natsDriver) getHashstatesData(ctx context.Context, path string) ([]byte, error) {
	pathInfo := d.pathRouter.ParsePath(path)
	objectName := fmt.Sprintf("hashstates-%s", pathInfo.Digest)
	reader, err := d.os.Get(ctx, objectName)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	
	return io.ReadAll(reader)
}

// buildAuthOptions creates NATS authentication options based on config
func buildAuthOptions(authConfig AuthConfig) ([]nats.Option, error) {
	var opts []nats.Option

	switch authConfig.Type {
	case "nkey":
		if authConfig.NKeyFile != "" {
			// Load NKey from file
			opt, err := nats.NkeyOptionFromSeed(authConfig.NKeyFile)
			if err != nil {
				return nil, fmt.Errorf("failed to load NKey from file: %w", err)
			}
			opts = append(opts, opt)
		} else if authConfig.NKeySeed != "" {
			// Use NKey seed directly
			kp, err := nkeys.FromSeed([]byte(authConfig.NKeySeed))
			if err != nil {
				return nil, fmt.Errorf("failed to create key pair from NKey seed: %w", err)
			}
			pub, err := kp.PublicKey()
			if err != nil {
				return nil, fmt.Errorf("failed to get public key from NKey seed: %w", err)
			}
			opts = append(opts, nats.Nkey(pub, func(nonce []byte) ([]byte, error) {
				sig, err := kp.Sign(nonce)
				if err != nil {
					return nil, fmt.Errorf("failed to sign nonce: %w", err)
				}
				return sig, nil
			}))
		} else {
			return nil, fmt.Errorf("NKey authentication requires either 'nkeyfile' or 'nkeyseed' configuration")
		}

	case "userpass":
		if authConfig.Username == "" || authConfig.Password == "" {
			return nil, fmt.Errorf("userpass authentication requires both 'username' and 'password' configuration")
		}
		opts = append(opts, nats.UserInfo(authConfig.Username, authConfig.Password))

	case "token":
		if authConfig.Token == "" {
			return nil, fmt.Errorf("token authentication requires 'token' configuration")
		}
		opts = append(opts, nats.Token(authConfig.Token))

	case "jwt":
		if authConfig.JWT == "" {
			return nil, fmt.Errorf("JWT authentication requires 'jwt' configuration")
		}
		if authConfig.NKeySeed == "" && authConfig.NKeyFile == "" {
			return nil, fmt.Errorf("JWT authentication requires NKey for signing (set 'nkeyseed' or 'nkeyfile')")
		}
		
		var sigHandler nats.SignatureHandler
		if authConfig.NKeyFile != "" {
			// Load NKey from file for signing
			seedBytes, err := os.ReadFile(authConfig.NKeyFile)
			if err != nil {
				return nil, fmt.Errorf("failed to read NKey file: %w", err)
			}
			kp, err := nkeys.FromSeed(seedBytes)
			if err != nil {
				return nil, fmt.Errorf("failed to create key pair from file: %w", err)
			}
			sigHandler = func(nonce []byte) ([]byte, error) {
				return kp.Sign(nonce)
			}
		} else {
			// Use NKey seed for signing
			kp, err := nkeys.FromSeed([]byte(authConfig.NKeySeed))
			if err != nil {
				return nil, fmt.Errorf("failed to create key pair from seed: %w", err)
			}
			sigHandler = func(nonce []byte) ([]byte, error) {
				return kp.Sign(nonce)
			}
		}
		opts = append(opts, nats.UserJWT(func() (string, error) {
			return authConfig.JWT, nil
		}, sigHandler))

	case "":
		// No authentication configured
		break

	default:
		return nil, fmt.Errorf("unsupported authentication type: %s", authConfig.Type)
	}

	// Handle credentials file (can be used with JWT)
	if authConfig.CredentialsFile != "" {
		opts = append(opts, nats.UserCredentials(authConfig.CredentialsFile))
	}

	return opts, nil
}

// buildTLSConfig creates TLS configuration based on config
func buildTLSConfig(tlsConfig TLSConfig) (*tls.Config, error) {
	config := &tls.Config{
		InsecureSkipVerify: tlsConfig.InsecureSkip,
	}

	if tlsConfig.ServerName != "" {
		config.ServerName = tlsConfig.ServerName
	}

	// Load client certificate and key
	if tlsConfig.CertFile != "" && tlsConfig.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(tlsConfig.CertFile, tlsConfig.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load client certificate from '%s' and key from '%s': %w", tlsConfig.CertFile, tlsConfig.KeyFile, err)
		}
		config.Certificates = []tls.Certificate{cert}
	}

	// Load CA certificate
	if tlsConfig.CAFile != "" {
		caCert, err := os.ReadFile(tlsConfig.CAFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read CA certificate from file '%s': %w", tlsConfig.CAFile, err)
		}
		
		caCertPool := config.RootCAs
		if caCertPool == nil {
			caCertPool = x509.NewCertPool()
		}
		
		if !caCertPool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("failed to parse CA certificate from file '%s' - invalid PEM format", tlsConfig.CAFile)
		}
		config.RootCAs = caCertPool
	}

	return config, nil
}

