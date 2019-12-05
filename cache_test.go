package influx

import (
	"errors"
	"reflect"
	"testing"
	"time"

	influxdb "github.com/influxdata/influxdb1-client/v2"
)

func TestPointsCache_Push(t *testing.T) {
	cache := NewCache()

	tests := []struct {
		name        string
		measurement Measurement
		getPoint    func() *influxdb.Point
		isValid     bool
	}{
		{
			name:        "Cache full queue error",
			measurement: Measurement{},
			getPoint:    func() *influxdb.Point { return nil },
		},
		{
			name:        "Valid push request",
			measurement: testMeasurement,
			getPoint: func() *influxdb.Point {
				tags := map[string]string{"tag": "value"}
				fields := map[string]interface{}{"field": "value"}
				point, _ := influxdb.NewPoint("test", tags, fields, time.Now())
				return point
			},
			isValid: true,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			err := cache.Push(test.measurement, test.getPoint())
			if !test.isValid {
				if err == nil {
					t.Fatal("expected error")
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
			}
		})
	}
}

func TestPointsCache_FetchAndFlush(t *testing.T) {
	var points []*influxdb.Point
	cache := NewCache()
	point, _ := newPoint(testMeasurement.Name, nil, nil, "")

	err := cache.Push(testMeasurement, point)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	tests := []struct {
		name        string
		measurement Measurement
		callback    func([]*influxdb.Point) error
		isValid     bool
	}{
		{
			name:        "Flush with error",
			measurement: testMeasurement,
			callback: func(p []*influxdb.Point) error {
				points = p
				return errors.New("flush")
			},
		},
		{
			name:        "Valid push request",
			measurement: testMeasurement,
			callback: func(p []*influxdb.Point) error {
				points = p
				return nil
			},
			isValid: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// no parallel

			queueBefore := cache.queue[test.measurement.Name]
			err := cache.FetchAndFlush(test.measurement, test.callback)
			queueAfter := cache.queue[test.measurement.Name]

			if !test.isValid {
				if err == nil {
					t.Fatal("expected error")
				}
				if !reflect.DeepEqual(queueBefore, queueAfter) {
					t.Fatalf("expected to match. want: %v, got: %v", queueBefore, queueAfter)
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if len(points) == 0 {
					t.Fatal("expected not to be empty")
				}
				if len(queueAfter) != 0 {
					t.Fatal("expected to be empty")
				}
			}
		})
	}
}
