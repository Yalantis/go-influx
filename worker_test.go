package influx

import (
	"runtime"
	"testing"
	"time"

	influxdb "github.com/influxdata/influxdb1-client/v2"
)

func TestInflux_StartWorker_StopWorker(t *testing.T) {
	client, err := influxdb.NewHTTPClient(cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	numGoroutineBefore := runtime.NumGoroutine()
	influx, _ := New(client, Config{})
	influx.RegisterMeasurement(testMeasurement)

	influx.Start()
	time.Sleep(time.Millisecond) // launch goroutine

	if !influx.reportersStarted {
		t.Fatalf("expected %q to be true", "workerStarted")
	}

	influx.Shutdown()
	time.Sleep(time.Millisecond) // shutdown goroutine

	numGoroutineAfter := runtime.NumGoroutine()

	if numGoroutineBefore != numGoroutineAfter {
		t.Fatalf("expected match. want: %v, got: %v", numGoroutineBefore, numGoroutineAfter)
	}
}
