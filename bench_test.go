package influx

import (
	"testing"
	"time"

	influxdb "github.com/influxdata/influxdb1-client/v2"
)

func BenchmarkInflux_SendPoints(b *testing.B) {
	influx, cancel := newInflux()
	defer cancel()

	influx.RegisterMeasurement(testMeasurement)

	tags := map[string]string{"tag": "value"}
	fields := map[string]interface{}{"field": "value"}
	point, _ := influxdb.NewPoint(testMeasurement.Name, tags, fields, time.Now())

	points := []*influxdb.Point{point}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = influx.SendPoints(testMeasurement, points)
	}
}
