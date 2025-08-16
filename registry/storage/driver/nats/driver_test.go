package nats

import (
	"testing"

	storagedriver "github.com/distribution/distribution/v3/registry/storage/driver"
	"github.com/distribution/distribution/v3/registry/storage/driver/testsuites"
)

func newDriverConstructor(tb testing.TB) testsuites.DriverConstructor {
	return func() (storagedriver.StorageDriver, error) {
		// Create a test driver instance but don't pass testing.T since we can't
		// handle cleanup in this context - the testsuites framework will handle cleanup
		d, cleanup := newTestDriverConstructor(tb.(*testing.T))
		// Store cleanup for later (though testsuites doesn't use it)
		_ = cleanup
		return d, nil
	}
}

func TestNATSDriverSuite(t *testing.T) {
	testsuites.Driver(t, newDriverConstructor(t), true)
}

func BenchmarkNATSDriverSuite(b *testing.B) {
	testsuites.BenchDriver(b, newDriverConstructor(b))
}