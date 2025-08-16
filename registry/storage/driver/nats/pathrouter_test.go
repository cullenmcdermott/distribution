package nats

import (
	"testing"
)

func TestPathRouter(t *testing.T) {
	router := NewPathRouter()
	
	testCases := []struct {
		path         string
		expectedType PathType
		expectedID   string
		description  string
	}{
		{
			path:         "/docker/registry/v2/repositories/hello-world/_uploads/abc123-def456/data",
			expectedType: PathTypeUploadData,
			expectedID:   "abc123-def456",
			description:  "upload data path",
		},
		{
			path:         "/docker/registry/v2/repositories/hello-world/_uploads/abc123-def456/hashstates/sha256/0",
			expectedType: PathTypeUploadHashstates,
			expectedID:   "abc123-def456",
			description:  "upload hashstates path",
		},
		{
			path:         "/docker/registry/v2/blobs/sha256/ab/cd1234567890123456789012345678901234567890123456789012345678ef",
			expectedType: PathTypeRegular,
			expectedID:   "",
			description:  "regular blob path",
		},
		{
			path:         "/docker/registry/v2/repositories/hello-world/manifests/latest",
			expectedType: PathTypeRegular,
			expectedID:   "",
			description:  "regular manifest path",
		},
	}
	
	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			result := router.ParsePath(tc.path)
			
			if result.Type != tc.expectedType {
				t.Errorf("Expected type %v, got %v for path %s", tc.expectedType, result.Type, tc.path)
			}
			
			if result.UploadID != tc.expectedID {
				t.Errorf("Expected upload ID %s, got %s for path %s", tc.expectedID, result.UploadID, tc.path)
			}
		})
	}
}

func TestPathRouterDigestExtraction(t *testing.T) {
	router := NewPathRouter()
	
	testCases := []struct {
		path           string
		expectedDigest string
		description    string
	}{
		{
			path:           "/docker/registry/v2/blobs/sha256/ab/cd1234567890123456789012345678901234567890123456789012345678ef",
			expectedDigest: "abcd1234567890123456789012345678901234567890123456789012345678ef",
			description:    "SHA256 path with proper digest",
		},
		{
			path:           "/docker/registry/v2/blobs/sha256/12/34567890abcdef1234567890abcdef1234567890abcdef123456789012efab",
			expectedDigest: "1234567890abcdef1234567890abcdef1234567890abcdef123456789012efab",
			description:    "SHA256 path with different digest",
		},
	}
	
	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			result := router.ParsePath(tc.path)
			
			if result.Digest != tc.expectedDigest {
				t.Errorf("Expected digest %s, got %s for path %s", tc.expectedDigest, result.Digest, tc.path)
			}
		})
	}
}