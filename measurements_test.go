package influx

import (
	"reflect"
	"testing"
	"time"
)

func TestFindByName(t *testing.T) {
	influx, cancel := newInflux()
	defer cancel()

	influx.RegisterMeasurement(testMeasurement)

	type expected struct {
		measurement Measurement
		ok          bool
	}

	tests := []struct {
		name     string
		mName    string
		expected expected
	}{
		{
			name:  "Measurement exists",
			mName: testMeasurement.Name,
			expected: expected{
				measurement: Measurement{
					Database:        "go_statistics",
					FlushInterval:   time.Second,
					Name:            "go_memstats",
					QueueSize:       1000,
					RetentionPolicy: "shortterm",
				},
				ok: true,
			},
		},
		{
			name:  "Measurement doesn't exist",
			mName: "test",
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			actual, ok := influx.measurements.FindByName(test.mName)

			if test.expected.ok != ok {
				t.Errorf("expected to be equal. want: %v, got: %v", test.expected.ok, ok)
			}

			if !reflect.DeepEqual(test.expected.measurement, actual) {
				t.Fatalf("expected to be equal. want: %v, got: %v", test.expected.measurement, actual)
			}
		})
	}
}
