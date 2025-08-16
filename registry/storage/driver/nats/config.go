package nats

import (
	"fmt"
	"reflect"
	"time"
)

// AuthConfig contains NATS authentication configuration
type AuthConfig struct {
	Type            string `json:"type"`                       // "nkey", "userpass", "token", "jwt"
	NKeyFile        string `json:"nkeyfile,omitempty"`         // Path to NKey file
	NKeySeed        string `json:"nkeyseed,omitempty"`         // NKey seed string
	Username        string `json:"username,omitempty"`         // Username for user/pass auth
	Password        string `json:"password,omitempty"`         // Password for user/pass auth
	Token           string `json:"token,omitempty"`            // Token for token auth
	JWT             string `json:"jwt,omitempty"`              // JWT for JWT auth
	CredentialsFile string `json:"credentialsfile,omitempty"` // Path to credentials file
}

// TLSConfig contains TLS configuration options
type TLSConfig struct {
	Enabled      bool   `json:"enabled"`                 // Enable TLS
	CertFile     string `json:"certfile,omitempty"`      // Client certificate file
	KeyFile      string `json:"keyfile,omitempty"`       // Client private key file
	CAFile       string `json:"cafile,omitempty"`        // CA certificate file
	ServerName   string `json:"servername,omitempty"`    // Server name for certificate verification
	InsecureSkip bool   `json:"insecureskip,omitempty"`  // Skip certificate verification
}

// TimeoutConfig contains timeout and retry configuration
type TimeoutConfig struct {
	Connect        time.Duration `json:"connect,omitempty"`        // Connection timeout
	RequestTimeout time.Duration `json:"requesttimeout,omitempty"` // Request timeout
	ReconnectWait  time.Duration `json:"reconnectwait,omitempty"`  // Time to wait between reconnect attempts
	MaxReconnect   int           `json:"maxreconnect,omitempty"`   // Maximum reconnect attempts (-1 for unlimited)
}

// PerformanceConfig contains performance tuning options
type PerformanceConfig struct {
	ChunkSize        int64 `json:"chunksize,omitempty"`        // Chunk size for large objects (default: 32MB)
	MaxConnections   int   `json:"maxconnections,omitempty"`   // Maximum concurrent connections
	BufferSize       int   `json:"buffersize,omitempty"`       // Buffer size for streaming operations
	CompressionLevel int   `json:"compressionlevel,omitempty"` // Compression level (0-9, 0=disabled)
}

// JetStreamConfig contains JetStream-specific configuration
type JetStreamConfig struct {
	Domain          string        `json:"domain,omitempty"`          // JetStream domain
	APIPrefix       string        `json:"apiprefix,omitempty"`       // API prefix
	EventPrefix     string        `json:"eventprefix,omitempty"`     // Event prefix
	MaxWait         time.Duration `json:"maxwait,omitempty"`         // Maximum wait time for requests
	PublishTimeout  time.Duration `json:"publishtimeout,omitempty"`  // Publish timeout
	AckWait         time.Duration `json:"ackwait,omitempty"`         // Acknowledgment wait time
	MaxAckPending   int           `json:"maxackpending,omitempty"`   // Maximum pending acknowledgments
	ReplicaCount    int           `json:"replicacount,omitempty"`    // Number of replicas for streams
	StorageType     string        `json:"storagetype,omitempty"`     // Storage type ("file" or "memory")
	RetentionPolicy string        `json:"retentionpolicy,omitempty"` // Retention policy ("limits", "interest", "workqueue")
}

// Config represents the complete NATS driver configuration
type Config struct {
	ServerURL   string             `json:"serverurl"`
	Bucket      string             `json:"bucket"`
	Auth        AuthConfig         `json:"auth,omitempty"`
	TLS         TLSConfig          `json:"tls,omitempty"`
	Timeouts    TimeoutConfig      `json:"timeouts,omitempty"`
	Performance PerformanceConfig  `json:"performance,omitempty"`
	JetStream   JetStreamConfig    `json:"jetstream,omitempty"`
}

// DefaultConfig returns a Config with sensible defaults
func DefaultConfig() Config {
	return Config{
		Timeouts: TimeoutConfig{
			Connect:        30 * time.Second,
			RequestTimeout: 60 * time.Second,
			ReconnectWait:  2 * time.Second,
			MaxReconnect:   -1, // Unlimited
		},
		Performance: PerformanceConfig{
			ChunkSize:        32 * 1024 * 1024, // 32MB
			MaxConnections:   10,
			BufferSize:       64 * 1024, // 64KB
			CompressionLevel: 0,         // Disabled
		},
		JetStream: JetStreamConfig{
			MaxWait:         30 * time.Second,
			PublishTimeout:  30 * time.Second,
			AckWait:         30 * time.Second,
			MaxAckPending:   1000,
			ReplicaCount:    1,
			StorageType:     "file",
			RetentionPolicy: "limits",
		},
	}
}

func parseConfig(parameters map[string]interface{}) (Config, error) {
	// Start with defaults
	config := DefaultConfig()

	// Parse required parameters
	serverURL, ok := parameters["serverurl"]
	if !ok {
		return config, fmt.Errorf("no serverurl parameter provided")
	}
	serverURLStr, ok := serverURL.(string)
	if !ok {
		return config, fmt.Errorf("serverurl parameter must be a string, received %v", reflect.TypeOf(serverURL))
	}
	if serverURLStr == "" {
		return config, fmt.Errorf("serverurl parameter cannot be empty")
	}
	config.ServerURL = serverURLStr

	bucket, ok := parameters["bucket"]
	if !ok {
		return config, fmt.Errorf("no bucket parameter provided")
	}
	bucketStr, ok := bucket.(string)
	if !ok {
		return config, fmt.Errorf("bucket parameter must be a string, received %v", reflect.TypeOf(bucket))
	}
	if bucketStr == "" {
		return config, fmt.Errorf("bucket parameter cannot be empty")
	}
	config.Bucket = bucketStr

	// Parse optional auth configuration
	if authParams, ok := parameters["auth"].(map[string]interface{}); ok {
		if authType, ok := authParams["type"].(string); ok {
			config.Auth.Type = authType
		}
		if nkeyFile, ok := authParams["nkeyfile"].(string); ok {
			config.Auth.NKeyFile = nkeyFile
		}
		if nkeySeed, ok := authParams["nkeyseed"].(string); ok {
			config.Auth.NKeySeed = nkeySeed
		}
		if username, ok := authParams["username"].(string); ok {
			config.Auth.Username = username
		}
		if password, ok := authParams["password"].(string); ok {
			config.Auth.Password = password
		}
		if token, ok := authParams["token"].(string); ok {
			config.Auth.Token = token
		}
		if jwt, ok := authParams["jwt"].(string); ok {
			config.Auth.JWT = jwt
		}
		if credsFile, ok := authParams["credentialsfile"].(string); ok {
			config.Auth.CredentialsFile = credsFile
		}
	}

	// Parse optional TLS configuration
	if tlsParams, ok := parameters["tls"].(map[string]interface{}); ok {
		if enabled, ok := tlsParams["enabled"].(bool); ok {
			config.TLS.Enabled = enabled
		}
		if certFile, ok := tlsParams["certfile"].(string); ok {
			config.TLS.CertFile = certFile
		}
		if keyFile, ok := tlsParams["keyfile"].(string); ok {
			config.TLS.KeyFile = keyFile
		}
		if caFile, ok := tlsParams["cafile"].(string); ok {
			config.TLS.CAFile = caFile
		}
		if serverName, ok := tlsParams["servername"].(string); ok {
			config.TLS.ServerName = serverName
		}
		if insecureSkip, ok := tlsParams["insecureskip"].(bool); ok {
			config.TLS.InsecureSkip = insecureSkip
		}
	}

	// Parse optional performance configuration
	if perfParams, ok := parameters["performance"].(map[string]interface{}); ok {
		if chunkSize, ok := perfParams["chunksize"].(float64); ok {
			config.Performance.ChunkSize = int64(chunkSize)
		}
		if maxConns, ok := perfParams["maxconnections"].(float64); ok {
			config.Performance.MaxConnections = int(maxConns)
		}
		if bufSize, ok := perfParams["buffersize"].(float64); ok {
			config.Performance.BufferSize = int(bufSize)
		}
		if compLevel, ok := perfParams["compressionlevel"].(float64); ok {
			config.Performance.CompressionLevel = int(compLevel)
		}
	}

	return config, nil
}