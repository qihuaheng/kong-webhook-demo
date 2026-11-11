// Package telemetry wires OpenTelemetry tracing and structured logging.
package telemetry

import (
	"context"
	"log/slog"
	"os"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

const tracerName = "webhookd"

// Tracer returns the process tracer (a no-op until Setup installs a provider).
func Tracer() trace.Tracer { return otel.Tracer(tracerName) }

// Setup configures JSON logging and, if otlpEndpoint is set, OTLP/gRPC trace
// export. The returned function flushes and shuts down the exporter.
func Setup(ctx context.Context, role, otlpEndpoint string) (func(context.Context) error, error) {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})).
		With("service", "webhookd", "role", role))
	otel.SetTextMapPropagator(propagation.TraceContext{})
	if otlpEndpoint == "" {
		return func(context.Context) error { return nil }, nil
	}
	exp, err := otlptracegrpc.New(ctx, otlptracegrpc.WithEndpoint(otlpEndpoint), otlptracegrpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	res, _ := resource.Merge(resource.Default(), resource.NewWithAttributes(semconv.SchemaURL,
		semconv.ServiceName("webhookd"), semconv.ServiceInstanceID(hostname())))
	tp := sdktrace.NewTracerProvider(sdktrace.WithBatcher(exp), sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(0.1))))
	otel.SetTracerProvider(tp)
	return tp.Shutdown, nil
}

// TraceParent serializes the span context in ctx as a W3C traceparent.
func TraceParent(ctx context.Context) string {
	c := propagation.MapCarrier{}
	propagation.TraceContext{}.Inject(ctx, c)
	return c["traceparent"]
}

// LinkFrom turns a stored traceparent into a span link (design §11: the
// deliver span links back to the ingest trace rather than being its child,
// since delivery can happen hours later).
func LinkFrom(traceParent string) []trace.Link {
	if traceParent == "" {
		return nil
	}
	ctx := propagation.TraceContext{}.Extract(context.Background(), propagation.MapCarrier{"traceparent": traceParent})
	sc := trace.SpanContextFromContext(ctx)
	if !sc.IsValid() {
		return nil
	}
	return []trace.Link{{SpanContext: sc}}
}

func hostname() string {
	h, _ := os.Hostname()
	return h
}
