package influx

import (
	"fmt"
	"sync"

	influxdb "github.com/influxdata/influxdb1-client/v2"
)

type pointsCache struct {
	mx    sync.Mutex
	queue map[string][]*influxdb.Point
}

// NewCache creates pointers cache
func NewCache() *pointsCache {
	return &pointsCache{
		queue: make(map[string][]*influxdb.Point),
	}
}

// Push adds point to points cache and if cache is not overflowed (measured by QueueSize)
// this method returns cache contents
func (pc *pointsCache) Push(m Measurement, point *influxdb.Point) error {
	pc.mx.Lock()
	defer pc.mx.Unlock()

	// init cache repository for new measurement
	if pc.queue[m.Name] == nil {
		pc.queue[m.Name] = make([]*influxdb.Point, 0, m.QueueSize)
	}

	if len(pc.queue[m.Name]) >= m.QueueSize {
		return fmt.Errorf("queue overflow: %s", m.Name)
	}

	// adding newly-created point to cache
	pc.queue[m.Name] = append(pc.queue[m.Name], point)

	return nil
}

// FetchAndFlush fetches points from cache and flush them if callback was success
func (pc *pointsCache) FetchAndFlush(m Measurement, cb func([]*influxdb.Point) error) error {
	// fetch points from cache
	pc.mx.Lock()
	points := pc.queue[m.Name]
	pc.mx.Unlock()

	// try to run callback. Skip flush points on error
	if err := cb(points); err != nil {
		return err
	}

	// flush old points
	// cleanup only old points and keep new ones
	pc.mx.Lock()
	newPoints := pc.queue[m.Name]
	diffSize := len(newPoints) - len(points)
	if diffSize > 0 {
		copy(newPoints, newPoints[len(points):])
	}
	if diffSize >= 0 {
		pc.queue[m.Name] = newPoints[:diffSize]
	}
	pc.mx.Unlock()

	return nil
}
