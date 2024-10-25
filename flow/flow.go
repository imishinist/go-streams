package flow

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	parallelismGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "go_streams_parallelism",
		Help: "The number of parallelism for stream",
	}, []string{"name", "type"})

	workersGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "go_streams_workers",
		Help: "The number of workers for stream",
	}, []string{"name", "type"})
)
