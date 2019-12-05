package influx

import "time"

const (
	// retention policy
	AutogenRP   = "autogen"
	ShortTermRP = "shortterm"
	// defaults
	DefaultQueueSize     = 1000
	DefaultFlushInterval = time.Second
)

type (
	// Measurement model
	Measurement struct {
		Name            string
		Precision       string
		Database        string
		RetentionPolicy string
		QueueSize       int
		FlushInterval   time.Duration
	}

	measurements []Measurement
)

// FindByName finds measurement by name among measurement collection
func (ms measurements) FindByName(name string) (Measurement, bool) {
	for _, m := range ms {
		if m.Name == name {
			return m, true
		}
	}
	return Measurement{}, false
}
