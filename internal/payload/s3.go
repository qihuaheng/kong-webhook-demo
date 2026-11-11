package payload

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/minio/minio-go/v7/pkg/lifecycle"
)

const maxObjectBytes = 4 << 20 // compressed payloads are <= 256KB; guard anyway

// S3Store is an ObjectStore backed by any S3-compatible service.
type S3Store struct {
	c      *minio.Client
	bucket string
}

func NewS3Store(endpoint, accessKey, secretKey, bucket, region string, useSSL bool) (*S3Store, error) {
	// minio-go's default transport keeps only 16 idle connections per host,
	// which causes connection churn (and a latency cliff) above a few
	// thousand PUT/s. Size the pool for ingest concurrency.
	tr, err := minio.DefaultTransport(useSSL)
	if err != nil {
		return nil, err
	}
	tr.MaxIdleConns = 2048
	tr.MaxIdleConnsPerHost = 1024
	c, err := minio.New(endpoint, &minio.Options{
		Creds:     credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure:    useSSL,
		Region:    region,
		Transport: tr,
	})
	if err != nil {
		return nil, err
	}
	return &S3Store{c: c, bucket: bucket}, nil
}

// EnsureBucket creates the bucket if needed and installs the expiration rule.
// In production the bucket and rule are typically provisioned by IaC; this is
// idempotent and safe to call on startup.
func (s *S3Store) EnsureBucket(ctx context.Context, retentionDays int) error {
	ok, err := s.c.BucketExists(ctx, s.bucket)
	if err != nil {
		return err
	}
	if !ok {
		if err := s.c.MakeBucket(ctx, s.bucket, minio.MakeBucketOptions{}); err != nil {
			return err
		}
	}
	cfg := lifecycle.NewConfiguration()
	cfg.Rules = []lifecycle.Rule{{
		ID:         "webhookd-payload-expiry",
		Status:     "Enabled",
		RuleFilter: lifecycle.Filter{Prefix: "p/"},
		Expiration: lifecycle.Expiration{Days: lifecycle.ExpirationDays(retentionDays)},
	}}
	return s.c.SetBucketLifecycle(ctx, s.bucket, cfg)
}

func (s *S3Store) Put(ctx context.Context, key string, data []byte) error {
	_, err := s.c.PutObject(ctx, s.bucket, key, bytes.NewReader(data), int64(len(data)),
		minio.PutObjectOptions{ContentType: "application/zstd", DisableMultipart: true})
	return err
}

func (s *S3Store) Get(ctx context.Context, key string) ([]byte, error) {
	obj, err := s.c.GetObject(ctx, s.bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, mapErr(err)
	}
	defer obj.Close()
	b, err := readAllLimit(obj, maxObjectBytes)
	if err != nil {
		return nil, mapErr(err)
	}
	return b, nil
}

// Ping checks connectivity for readiness probes.
func (s *S3Store) Ping(ctx context.Context) error {
	_, err := s.c.BucketExists(ctx, s.bucket)
	return err
}

func mapErr(err error) error {
	var er minio.ErrorResponse
	if errors.As(err, &er) && (er.StatusCode == http.StatusNotFound || er.Code == "NoSuchKey") {
		return ErrNotFound
	}
	return fmt.Errorf("payload: s3: %w", err)
}
