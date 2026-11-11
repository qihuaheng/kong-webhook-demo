// Command loadgen drives webhookd at a target rate and verifies the
// end-state invariant: every accepted delivery is eventually succeeded or
// dead, and the receiver saw every webhook-id (at-least-once).
//
//	go run ./test/load -rate 2000 -duration 60s -size 4096 -endpoints 20 -fail 0.1
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"webhookd/pkg/sdk"
)

func main() {
	api := flag.String("api", "http://127.0.0.1:8080", "webhookd API")
	recv := flag.String("recv", "http://127.0.0.1:8090", "mock receiver (from this host)")
	recvInternal := flag.String("recv-internal", "http://10.89.50.100:8090", "mock receiver (from workers)")
	admin := flag.String("admin-key", "dev-admin-key", "admin key")
	rate := flag.Int("rate", 1000, "events per second")
	duration := flag.Duration("duration", 30*time.Second, "test duration")
	size := flag.Int("size", 4096, "payload bytes (approx)")
	endpoints := flag.Int("endpoints", 10, "number of endpoints (fan-out 1, round-robin event types)")
	failP := flag.Float64("fail", 0, "receiver 500 probability")
	conc := flag.Int("concurrency", 256, "concurrent publishers")
	drain := flag.Duration("drain", 3*time.Minute, "max time to wait for all deliveries to finish")
	flag.Parse()

	ctx := context.Background()
	httpc := &http.Client{Timeout: 30 * time.Second, Transport: &http.Transport{MaxIdleConnsPerHost: *conc}}
	adm := sdk.New(*api, *admin)
	adm.HTTP = httpc
	_, key, err := adm.CreateTenant(ctx, fmt.Sprintf("load-%d", time.Now().Unix()), nil)
	must(err)
	c := sdk.New(*api, key)
	c.HTTP = httpc
	must(post(httpc, *recv+"/_reset", ""))

	q := ""
	if *failP > 0 {
		q = fmt.Sprintf("?fail=%g", *failP)
	}
	retry := &sdk.RetryPolicy{BaseDelay: "1s", Multiplier: ptr(1.5), MaxDelay: "10s", MaxAttempts: ptr(10)}
	for i := 0; i < *endpoints; i++ {
		name := fmt.Sprintf("load%d", i)
		_, secret, err := c.CreateEndpoint(ctx, sdk.EndpointInput{URL: ptr(*recvInternal + "/hook/" + name + q),
			EventTypes: []string{fmt.Sprintf("load.e%d", i)}, RetryPolicy: retry})
		must(err)
		must(put(httpc, *recv+"/_secrets/"+name, secret))
	}

	blob := make([]byte, *size/2)
	rand.Read(blob)
	data := map[string]string{"blob": hex.EncodeToString(blob)}

	var sent, failed atomic.Int64
	var mu sync.Mutex
	var lat []time.Duration
	ids := make(chan string, 1<<22)
	work := make(chan int, *conc)
	var wg sync.WaitGroup
	for range *conc {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range work {
				t0 := time.Now()
				res, err := c.SendEvent(ctx, fmt.Sprintf("load.e%d", i%*endpoints), data, nil)
				d := time.Since(t0)
				if err != nil {
					failed.Add(1)
					continue
				}
				sent.Add(1)
				for _, id := range res.DeliveryIDs {
					ids <- id
				}
				mu.Lock()
				lat = append(lat, d)
				mu.Unlock()
			}
		}()
	}
	log.Printf("publishing %d ev/s for %s, %d endpoints, payload ~%dB, receiver fail=%g", *rate, *duration, *endpoints, *size, *failP)
	start := time.Now()
	dropped := 0
	// Pace by elapsed time (1ms ticks) rather than one tick per event:
	// sub-millisecond tickers are unreliable and would cap the rate.
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	issued := 0
	for time.Since(start) < *duration {
		<-tick.C
		due := int(time.Since(start).Seconds() * float64(*rate))
		for ; issued < due; issued++ {
			select {
			case work <- issued:
			default:
				dropped++ // publishers saturated: the generator, not the server, is the bottleneck
			}
		}
	}
	close(work)
	wg.Wait()
	elapsed := time.Since(start)
	close(ids)
	all := make([]string, 0, len(ids))
	for id := range ids {
		all = append(all, id)
	}
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	p := func(x float64) time.Duration {
		if len(lat) == 0 {
			return 0
		}
		return lat[min(len(lat)-1, int(float64(len(lat))*x))]
	}
	fmt.Printf("\nINGEST  accepted=%d errors=%d generator_dropped=%d achieved=%.0f ev/s  p50=%s p99=%s max=%s\n",
		sent.Load(), failed.Load(), dropped, float64(sent.Load())/elapsed.Seconds(), p(.5), p(.99), p(1))

	// Drain: wait until the receiver has acknowledged every id or retries run out.
	deadline := time.Now().Add(*drain)
	var st recvStats
	for time.Now().Before(deadline) {
		st = stats(httpc, *recv)
		if st.UniqueAcked >= int64(len(all)) {
			break
		}
		time.Sleep(time.Second)
	}
	fmt.Printf("RECEIVE requests=%d verified=%d bad_sig=%d unique_ids=%d acked_2xx=%d duplicates_after_2xx=%d e2e_p50=%.0fms e2e_p99=%.0fms\n",
		st.Requests, st.Verified, st.BadSignature, st.UniqueIDs, st.Responded2xx, st.Duplicates, st.P50, st.P99)

	// Verify final states for a sample (all if small) via the API.
	sample := all
	if len(sample) > 2000 {
		sample = sample[:2000]
	}
	counts := map[string]int{}
	for _, id := range sample {
		d, err := c.GetDelivery(ctx, id)
		if err != nil {
			counts["error"]++
			continue
		}
		counts[d.Status]++
	}
	fmt.Printf("STATES  (sample %d) %v\n", len(sample), counts)
	ok := st.UniqueAcked+int64(counts["dead"]) >= int64(len(all)) && counts["pending"]+counts["retrying"]+counts["error"] == 0 && st.BadSignature == 0
	if !ok {
		fmt.Println("RESULT  FAIL: not every accepted delivery reached a terminal state")
		os.Exit(1)
	}
	fmt.Println("RESULT  PASS: every accepted delivery reached a terminal state; all signatures valid")
}

type recvStats struct {
	Requests     int64   `json:"requests"`
	Verified     int64   `json:"verified"`
	BadSignature int64   `json:"bad_signature"`
	UniqueIDs    int64   `json:"unique_ids"`
	Responded2xx int64   `json:"responded_2xx"`
	Duplicates   int64   `json:"duplicates_after_2xx"`
	P50          float64 `json:"e2e_latency_p50_ms"`
	P99          float64 `json:"e2e_latency_p99_ms"`
	UniqueAcked  int64   `json:"-"`
}

func stats(c *http.Client, base string) recvStats {
	var s recvStats
	resp, err := c.Get(base + "/_stats")
	if err != nil {
		return s
	}
	defer resp.Body.Close()
	json.NewDecoder(resp.Body).Decode(&s)
	s.UniqueAcked = s.Responded2xx - s.Duplicates
	return s
}

func post(c *http.Client, u, body string) error { return do(c, http.MethodPost, u, body) }
func put(c *http.Client, u, body string) error  { return do(c, http.MethodPut, u, body) }

func do(c *http.Client, m, u, body string) error {
	req, _ := http.NewRequest(m, u, strings.NewReader(body))
	resp, err := c.Do(req)
	if err != nil {
		return err
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("%s %s: %d", m, u, resp.StatusCode)
	}
	return nil
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func ptr[T any](v T) *T { return &v }
