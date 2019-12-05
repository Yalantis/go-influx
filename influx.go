package influx

import (
	"sync"
	"time"

	"github.com/pkg/errors"

	influxdb "github.com/influxdata/influxdb1-client/v2"
)

// Logger interface
type Logger interface {
	Error(...interface{})
}

// Config configure reporting
type Config struct {
	Postfix  string
	Hostname string
}

// Influx is a wrapper around influxdb.Client
type Influx struct {
	wg               *sync.WaitGroup
	config           Config
	client           influxdb.Client
	cache            *pointsCache
	logger           Logger
	measurements     measurements
	reportersStarted bool
	shutdown         chan struct{}
}

const DefaultPrecision = "ns"

// New initiates connection with Influx
func New(client influxdb.Client, config Config) (*Influx, error) {
	_, _, err := client.Ping(0)
	if err != nil {
		return nil, err
	}

	return &Influx{
		wg:       &sync.WaitGroup{},
		config:   config,
		client:   client,
		cache:    NewCache(),
		shutdown: make(chan struct{}),
	}, nil
}

// SetLogger sets logger
func (i *Influx) SetLogger(logger Logger) {
	i.logger = logger
}

// Start starts Influx reporting worker for each measurement concurrently
// each worker tries to fetch measurements from cache and report them every FlushInterval
func (i *Influx) Start() {
	i.startReporters(i.measurements)
	i.reportersStarted = true
}

// Shutdown stops reporting worker
func (i *Influx) Shutdown() {
	close(i.shutdown)
	i.wg.Wait()
	_ = i.client.Close()
}

// RegisterMeasurement registers measurement
func (i *Influx) RegisterMeasurement(m ...Measurement) {
	for _, m := range m {
		if m.FlushInterval == 0 {
			m.FlushInterval = DefaultFlushInterval
		}
	}

	i.measurements = append(i.measurements, m...)

	if i.reportersStarted {
		// start reporters for new measurements, if worker started
		i.startReporters(m)
	}
}

// Push adds measurement to cache
func (i *Influx) Push(name string, tags map[string]string, fields map[string]interface{}) error {
	// finishing if Influx is not enabled
	if i == nil {
		return nil
	}

	// find measurement settings
	m, ok := i.measurements.FindByName(name)
	if !ok {
		return errors.Errorf("no measurements with name { %s } were found", name)
	}

	point, err := newPoint(name, tags, fields, i.config.Hostname)
	if err != nil {
		return err
	}

	// write point to cache
	return i.cache.Push(m, point)
}

// SendPoints sends measurement points to influxDB server. If no points - do nothing
func (i *Influx) SendPoints(m Measurement, points []*influxdb.Point) error {
	if len(points) == 0 {
		return nil
	}

	if m.Precision == "" {
		m.Precision = DefaultPrecision
	}

	bp, err := influxdb.NewBatchPoints(influxdb.BatchPointsConfig{
		Precision:       m.Precision,
		Database:        m.Database,
		RetentionPolicy: m.RetentionPolicy,
	})
	if err != nil {
		return err
	}

	bp.AddPoints(points)

	return i.client.Write(bp)
}

func (i *Influx) fetchAndFlush(m Measurement) {
	err := i.cache.FetchAndFlush(m, func(points []*influxdb.Point) error {
		return i.SendPoints(m, points)
	})

	if err != nil && i.logger != nil {
		i.logger.Error("send points", "error", err)
	}
}

// newPoint creates new Point and sets default values
func newPoint(name string, tags map[string]string, fields map[string]interface{}, host string) (*influxdb.Point, error) {
	if tags == nil {
		tags = make(map[string]string)
	}

	if fields == nil {
		fields = make(map[string]interface{})
	}

	// adding hostname
	tags["host"] = host
	// adding statistical information
	fields["value"] = 1

	return influxdb.NewPoint(name, tags, fields, time.Now())
}
