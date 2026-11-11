package secretbox

import (
	"bytes"
	"crypto/rand"
	"testing"
)

func TestRoundTripAndAAD(t *testing.T) {
	key := make([]byte, 32)
	rand.Read(key)
	b, err := New(key)
	if err != nil {
		t.Fatal(err)
	}
	ct := b.Seal([]byte("whsec_abc"), []byte("ep_1"))
	pt, err := b.Open(ct, []byte("ep_1"))
	if err != nil || !bytes.Equal(pt, []byte("whsec_abc")) {
		t.Fatalf("roundtrip failed: %v", err)
	}
	if _, err := b.Open(ct, []byte("ep_2")); err == nil {
		t.Fatal("wrong aad must fail")
	}
	ct[len(ct)-1] ^= 1
	if _, err := b.Open(ct, []byte("ep_1")); err == nil {
		t.Fatal("tampered ciphertext must fail")
	}
	if _, err := New(key[:16]); err == nil {
		t.Fatal("short key must fail")
	}
}
