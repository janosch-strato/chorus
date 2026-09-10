package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

func counter(t *testing.T, name string, labels ...string) *dto.Counter {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.GetMetric() {
			values := map[string]bool{}
			for _, label := range metric.GetLabel() {
				values[label.GetValue()] = true
			}
			found := true
			for _, want := range labels {
				if !values[want] {
					found = false
					break
				}
			}
			if found {
				return metric.GetCounter()
			}
		}
	}
	return nil
}

func Test_ProxyDestinationReadTimeout(t *testing.T) {
	r := require.New(t)
	ProxyDestinationReadTimeout("destination")
	ProxyDestinationReadTimeout("destination")

	c := counter(t, "proxy_destination_read_timeouts_total", "destination")
	r.NotNil(c, "series not exported")
	r.EqualValues(2, c.GetValue())

	// a destination that answers is not counted here
	r.Nil(counter(t, "proxy_destination_read_timeouts_total", "other-destination"))
}
