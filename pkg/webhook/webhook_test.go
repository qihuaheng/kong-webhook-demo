package webhook

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
)

// Test vector from the Standard Webhooks reference implementations.
const (
	vecSecret = "whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw"
	vecID     = "msg_p5jXN8AQM9LWM0D4loKWxJek"
	vecTS     = int64(1614265330)
	vecBody   = `{"test": 2432232314}`
	vecSig    = "v1,g0hM9SsE+OTPJTGt/tmIKtSyZlE3uFJELVlNIOLJ1OE="
)

func headers(id string, ts int64, sig string) http.Header {
	h := http.Header{}
	h.Set(HeaderID, id)
	h.Set(HeaderTimestamp, strconv.FormatInt(ts, 10))
	h.Set(HeaderSignature, sig)
	return h
}

func TestSignVector(t *testing.T) {
	s, err := NewHMACSigner(vecSecret)
	if err != nil {
		t.Fatal(err)
	}
	if got := s.Sign(vecID, vecTS, []byte(vecBody)); got != vecSig {
		t.Fatalf("got %s want %s", got, vecSig)
	}
}

func TestVerify(t *testing.T) {
	now := time.Unix(vecTS, 0)
	body := []byte(vecBody)
	ok := headers(vecID, vecTS, vecSig)
	if err := verifyAt(vecSecret, ok, body, 5*time.Minute, now); err != nil {
		t.Fatalf("valid: %v", err)
	}
	// Rotation: any matching signature among several passes.
	multi := headers(vecID, vecTS, "v1,AAAA "+vecSig)
	if err := verifyAt(vecSecret, multi, body, 5*time.Minute, now); err != nil {
		t.Fatalf("multi: %v", err)
	}
	cases := map[string]struct {
		h    http.Header
		body []byte
		now  time.Time
		want error
	}{
		"tampered body":  {ok, []byte(`{"test": 1}`), now, ErrNoMatch},
		"wrong id":       {headers("msg_x", vecTS, vecSig), body, now, ErrNoMatch},
		"too old":        {ok, body, now.Add(6 * time.Minute), ErrInvalidTimestamp},
		"future":         {ok, body, now.Add(-6 * time.Minute), ErrInvalidTimestamp},
		"missing":        {http.Header{}, body, now, ErrMissingHeaders},
		"unknown scheme": {headers(vecID, vecTS, "v1a,"+vecSig[3:]), body, now, ErrNoMatch},
	}
	for name, c := range cases {
		if err := verifyAt(vecSecret, c.h, c.body, 5*time.Minute, c.now); err != c.want {
			t.Errorf("%s: got %v want %v", name, err, c.want)
		}
	}
}

func TestSignatureHeaderRotation(t *testing.T) {
	oldS, newS := NewSecret(), NewSecret()
	a, _ := NewHMACSigner(newS)
	b, _ := NewHMACSigner(oldS)
	ts := time.Now().Unix()
	h := headers("msg_1", ts, SignatureHeader([]Signer{a, b}, "msg_1", ts, []byte("x")))
	for _, s := range []string{oldS, newS} {
		if err := Verify(s, h, []byte("x"), time.Minute); err != nil {
			t.Fatalf("secret should verify: %v", err)
		}
	}
	if err := Verify(NewSecret(), h, []byte("x"), time.Minute); err != ErrNoMatch {
		t.Fatalf("unrelated secret: %v", err)
	}
}

func TestVerifyRequestZstd(t *testing.T) {
	secret := NewSecret()
	s, _ := NewHMACSigner(secret)
	body := bytes.Repeat([]byte(`{"k":"v"}`), 1000)
	ts := time.Now().Unix()
	enc, _ := zstd.NewWriter(nil)
	compressed := enc.EncodeAll(body, nil)

	r := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(compressed))
	r.Header = headers("msg_z", ts, s.Sign("msg_z", ts, body))
	r.Header.Set("Content-Encoding", "zstd")
	got, err := VerifyRequest(secret, r, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, body) {
		t.Fatal("decoded body mismatch")
	}
}
