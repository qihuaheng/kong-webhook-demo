package metrics_test

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"

	_ "webhookd/internal/metrics"
)

// Guards against reintroducing unbounded label dimensions (design §17.4 P0-5).
// Label values must come from a closed set or from tenant count (bounded by
// requirement); never from producer- or customer-controlled strings, and
// never per endpoint.
func TestNoUnboundedLabels(t *testing.T) {
	allowed := map[string]bool{
		"tenant": true, "result": true, "status_class": true, "reason": true,
		"stage": true, "shard": true, "op": true, "to": true,
	}
	mfs, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatal(err)
	}
	// Vec metrics without observations are not gathered, so instead inspect
	// the registered descriptors.
	ch := make(chan *prometheus.Desc, 256)
	go func() {
		prometheus.DefaultRegisterer.(*prometheus.Registry).Describe(ch)
		close(ch)
	}()
	seen := 0
	for d := range ch {
		s := d.String()
		if !strings.Contains(s, `fqName: "webhook_`) {
			continue
		}
		seen++
		i := strings.Index(s, "variableLabels: {")
		if i < 0 {
			continue
		}
		rest := s[i+len("variableLabels: {"):]
		rest = rest[:strings.Index(rest, "}")]
		for _, l := range strings.Split(rest, ",") {
			if l = strings.TrimSpace(l); l != "" && !allowed[l] {
				t.Errorf("%s: label %q is not in the bounded allowlist", s[:strings.Index(s, ",")], l)
			}
		}
	}
	if seen < 10 {
		t.Fatalf("only %d webhook_ metrics described (gathered %d families)", seen, len(mfs))
	}
}
