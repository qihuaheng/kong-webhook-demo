// Command mockreceiver is a webhook receiver for demos, e2e, and load tests.
// It verifies signatures, detects duplicate deliveries, and can simulate
// failures. Not for production use.
//
//	POST /hook/{name}                 200 (behaviour tunable via query below)
//	     ?fail=0.3                    respond 500 with this probability
//	     ?status=503                  always respond with this status
//	     ?slow=2s                     sleep before responding
//	     ?flaky=2                     fail the first N attempts of each webhook-id with 503
//	     ?redirect=1                  307 to the same path without ?redirect
//	PUT  /_secrets/{name}             body = endpoint secret used to verify /hook/{name}
//	GET  /_stats[/{name}]             counters as JSON
//	POST /_reset                      clear counters
package main

import (
	"encoding/json"
	"io"
	"log"
	"math/rand/v2"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"webhookd/pkg/webhook"
)

type stats struct {
	Requests     int64   `json:"requests"`
	Verified     int64   `json:"verified"`
	BadSignature int64   `json:"bad_signature"`
	Responded2xx int64   `json:"responded_2xx"`
	UniqueIDs    int64   `json:"unique_ids"`
	Duplicates   int64   `json:"duplicates_after_2xx"` // re-deliveries of an already-acknowledged id
	LatencyP50ms float64 `json:"e2e_latency_p50_ms"`
	LatencyP99ms float64 `json:"e2e_latency_p99_ms"`
}

type bucket struct {
	stats
	seen    map[string]int  // webhook-id -> attempts seen
	acked   map[string]bool // webhook-id -> we answered 2xx
	latency []float64       // ms, first 2xx per id: now - envelope.timestamp
}

type server struct {
	mu      sync.Mutex
	secrets map[string]string
	b       map[string]*bucket
}

func (s *server) bucket(name string) *bucket {
	b, ok := s.b[name]
	if !ok {
		b = &bucket{seen: map[string]int{}, acked: map[string]bool{}}
		s.b[name] = b
	}
	return b
}

func (s *server) hook(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	q := r.URL.Query()
	s.mu.Lock()
	secret := s.secrets[name]
	s.mu.Unlock()

	var body []byte
	var verr error
	if secret != "" {
		body, verr = webhook.VerifyRequest(secret, r, 5*time.Minute)
	} else {
		body, _ = io.ReadAll(io.LimitReader(r.Body, 2<<20))
	}
	id := r.Header.Get(webhook.HeaderID)

	if q.Get("redirect") != "" {
		q.Del("redirect")
		u := *r.URL
		u.RawQuery = q.Encode()
		http.Redirect(w, r, u.String(), http.StatusTemporaryRedirect)
		return
	}
	if d, err := time.ParseDuration(q.Get("slow")); err == nil {
		time.Sleep(d)
	}

	s.mu.Lock()
	b := s.bucket(name)
	b.Requests++
	if secret != "" {
		if verr == nil {
			b.Verified++
		} else {
			b.BadSignature++
		}
	}
	n := b.seen[id]
	b.seen[id] = n + 1
	code := http.StatusOK
	if v, err := strconv.Atoi(q.Get("status")); err == nil {
		code = v
	} else if f, err := strconv.ParseFloat(q.Get("fail"), 64); err == nil && rand.Float64() < f {
		code = http.StatusInternalServerError
	} else if k, err := strconv.Atoi(q.Get("flaky")); err == nil && n < k {
		code = http.StatusServiceUnavailable
	}
	if secret != "" && verr != nil {
		code = http.StatusUnauthorized
	}
	if code >= 200 && code < 300 {
		b.Responded2xx++
		if b.acked[id] {
			b.Duplicates++
		} else {
			b.acked[id] = true
			var env struct {
				Timestamp time.Time `json:"timestamp"`
			}
			if json.Unmarshal(body, &env) == nil && !env.Timestamp.IsZero() {
				b.latency = append(b.latency, float64(time.Since(env.Timestamp).Microseconds())/1000)
			}
		}
	}
	s.mu.Unlock()
	w.WriteHeader(code)
}

func (s *server) putSecret(w http.ResponseWriter, r *http.Request) {
	b, _ := io.ReadAll(io.LimitReader(r.Body, 1024))
	s.mu.Lock()
	s.secrets[r.PathValue("name")] = strings.TrimSpace(string(b))
	s.mu.Unlock()
	w.WriteHeader(http.StatusNoContent)
}

func pct(v []float64, p float64) float64 {
	if len(v) == 0 {
		return 0
	}
	c := append([]float64(nil), v...)
	sort.Float64s(c)
	return c[min(len(c)-1, int(float64(len(c))*p))]
}

func (s *server) snapshot(names ...string) stats {
	var out stats
	var lat []float64
	for name, b := range s.b {
		if len(names) > 0 && names[0] != name {
			continue
		}
		out.Requests += b.Requests
		out.Verified += b.Verified
		out.BadSignature += b.BadSignature
		out.Responded2xx += b.Responded2xx
		out.UniqueIDs += int64(len(b.seen))
		out.Duplicates += b.Duplicates
		lat = append(lat, b.latency...)
	}
	out.LatencyP50ms, out.LatencyP99ms = pct(lat, 0.5), pct(lat, 0.99)
	return out
}

func main() {
	s := &server{secrets: map[string]string{}, b: map[string]*bucket{}}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /hook/{name}", s.hook)
	mux.HandleFunc("PUT /_secrets/{name}", s.putSecret)
	mux.HandleFunc("GET /_stats", func(w http.ResponseWriter, _ *http.Request) {
		s.mu.Lock()
		defer s.mu.Unlock()
		json.NewEncoder(w).Encode(s.snapshot())
	})
	mux.HandleFunc("GET /_stats/{name}", func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		defer s.mu.Unlock()
		json.NewEncoder(w).Encode(s.snapshot(r.PathValue("name")))
	})
	mux.HandleFunc("POST /_reset", func(w http.ResponseWriter, _ *http.Request) {
		s.mu.Lock()
		s.b = map[string]*bucket{}
		s.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	})
	addr := os.Getenv("RECEIVER_ADDR")
	if addr == "" {
		addr = ":8090"
	}
	log.Printf("mockreceiver listening on %s", addr)
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	log.Fatal(srv.ListenAndServe())
}
