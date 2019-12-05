package influx

import "time"

// startReporters starts reporter per measurement
func (i *Influx) startReporters(measurements []Measurement) {
	for _, m := range measurements {
		go i.reporter(m)
	}
}

// reporter is worker for fetch measurements and send it to influxDB
func (i *Influx) reporter(m Measurement) {
	i.wg.Add(1)
	t := time.NewTicker(m.FlushInterval)

loop:
	for {
		select {
		case <-t.C:
			i.fetchAndFlush(m)
		case <-i.shutdown:
			break loop
		}
	}

	t.Stop()
	// flush before stop
	i.fetchAndFlush(m)
	i.wg.Done()
}
