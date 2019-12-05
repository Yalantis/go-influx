package influx

import (
	"testing"
	"time"

	influxdb "github.com/influxdata/influxdb1-client/v2"
)

func TestInflux_StartWorker_StopWorker(t *testing.T) {
	client, err := influxdb.NewHTTPClient(cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	influx, _ := New(client, Config{})
	influx.RegisterMeasurement(testMeasurement)

	influx.Start()
	time.Sleep(time.Millisecond) // launch goroutine

	if !influx.reportersStarted {
		t.Fatalf("expected %q to be true", "workerStarted")
	}

	shutdown := make(chan struct{})

	go func() {
		influx.Shutdown()
		close(shutdown)
	}()

	select {
	case <-shutdown:
	case <-time.After(time.Second):
		t.Fatal("shutdown deadline exceeded")
	}
}
