package nats

import (
	"testing"
)

func TestParseConfig(t *testing.T) {
	tests := []struct {
		name        string
		parameters  map[string]interface{}
		expected    Config
		expectError bool
	}{
		{
			name: "valid config",
			parameters: map[string]interface{}{
				"serverurl": "nats://localhost:4222",
				"bucket":    "test-bucket",
			},
			expected: Config{
				ServerURL: "nats://localhost:4222",
				Bucket:    "test-bucket",
			},
			expectError: false,
		},
		{
			name:        "missing serverurl",
			parameters:  map[string]interface{}{"bucket": "test-bucket"},
			expectError: true,
		},
		{
			name:        "missing bucket",
			parameters:  map[string]interface{}{"serverurl": "nats://localhost:4222"},
			expectError: true,
		},
		{
			name: "empty serverurl",
			parameters: map[string]interface{}{
				"serverurl": "",
				"bucket":    "test-bucket",
			},
			expectError: true,
		},
		{
			name: "empty bucket",
			parameters: map[string]interface{}{
				"serverurl": "nats://localhost:4222",
				"bucket":    "",
			},
			expectError: true,
		},
		{
			name: "wrong type serverurl",
			parameters: map[string]interface{}{
				"serverurl": 123,
				"bucket":    "test-bucket",
			},
			expectError: true,
		},
		{
			name: "wrong type bucket",
			parameters: map[string]interface{}{
				"serverurl": "nats://localhost:4222",
				"bucket":    123,
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config, err := parseConfig(tt.parameters)
			
			if tt.expectError {
				if err == nil {
					t.Errorf("expected error, got nil")
				}
				return
			}
			
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			
			if config.ServerURL != tt.expected.ServerURL {
				t.Errorf("expected ServerURL %q, got %q", tt.expected.ServerURL, config.ServerURL)
			}
			
			if config.Bucket != tt.expected.Bucket {
				t.Errorf("expected Bucket %q, got %q", tt.expected.Bucket, config.Bucket)
			}
		})
	}
}