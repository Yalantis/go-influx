package influx

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	influxdb "github.com/influxdata/influxdb1-client/v2"
)

type fakeLogger struct {
	ErrorFn func()
}

func (f fakeLogger) Error(...interface{}) {
	if f.ErrorFn != nil {
		f.ErrorFn()
	}
}

var cfg = influxdb.HTTPConfig{
	Timeout: time.Second, // limit timeout for tests purpose
}

func init() {
	// mock influx server
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/ping" {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(influxdb.Response{})
	}))

	cfg.Addr = ts.URL
}

func TestNew(t *testing.T) {
	client, err := influxdb.NewHTTPClient(cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	influx, err := New(client, Config{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if influx == nil {
		t.Fatal("expected not to be nil")
	}
}

func TestInflux_SetLogger(t *testing.T) {
	influx, cancel := newInflux()
	defer cancel()

	var logged bool

	influx.SetLogger(fakeLogger{
		ErrorFn: func() {
			logged = true
		},
	})

	influx.logger.Error()

	if logged != true {
		t.Fatal("expected to be true")
	}
}

func TestInflux_Push(t *testing.T) {
	influx, cancel := newInflux()
	defer cancel()

	influx.RegisterMeasurement(testMeasurement)

	err := influx.Push(testMeasurement.Name, nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestInflux_SendPoints(t *testing.T) {
	influx, cancel := newInflux()
	defer cancel()

	influx.RegisterMeasurement(testMeasurement)

	tests := []struct {
		name        string
		measurement Measurement
		getPoints   func() []*influxdb.Point
	}{
		{
			name:        "with nil points",
			measurement: testMeasurement,
			getPoints:   func() []*influxdb.Point { return nil },
		},
		{
			name:        "with payload",
			measurement: testMeasurement,
			getPoints: func() []*influxdb.Point {
				tags := map[string]string{"tag": "value"}
				fields := map[string]interface{}{"field": "value"}
				point, _ := influxdb.NewPoint("test", tags, fields, time.Now())
				return []*influxdb.Point{point}
			},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			err := influx.SendPoints(test.measurement, test.getPoints())
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestNewPoint(t *testing.T) {
	actual, _ := newPoint("name", nil, nil, "host")
	if actual.Name() != "name" {
		t.Fatalf("expected to be equal. want: %v, got: %v", "name", actual.Name())
	}
	if actual.Tags() == nil {
		t.Fatal("expected not to be nil")
	}
	if actual.Tags()["host"] != "host" {
		t.Fatalf("expected to match. want: %v, got: %v", "host", actual.Tags()["host"])
	}
	fields, err := actual.Fields()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if fields == nil {
		t.Fatal("expected not to be nil")
	}
	if fields["value"] != int64(1) {
		t.Fatalf("expected to match. want: %v, got: %v", 1, fields["value"])
	}
	if actual.Time().IsZero() {
		t.Fatal("expected time not be zero")
	}
}

var testMeasurement = Measurement{
	Name:            "go_memstats",
	Database:        "go_statistics",
	RetentionPolicy: ShortTermRP,
	QueueSize:       DefaultQueueSize,
	FlushInterval:   DefaultFlushInterval,
}

func newInflux() (*Influx, func()) {
	client, err := influxdb.NewHTTPClient(cfg)
	if err != nil {
		panic(err)
	}

	influx, err := New(client, Config{})
	if err != nil {
		panic(err)
	}

	influx.Start()

	return influx, influx.Shutdown
}
