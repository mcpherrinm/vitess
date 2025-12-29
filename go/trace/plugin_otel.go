package trace

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdkTrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.37.0"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/stats"
)

func init() {
	tracingBackendFactories["opentelemetry"] = newOpenTelemetryFromEnv
}

func newOpenTelemetryFromEnv(serviceName string) (tracingService, io.Closer, error) {
	exp, err := otlptracegrpc.New(context.Background())
	if err != nil {
		return nil, nil, err
	}

	resources := resource.NewWithAttributes(
		semconv.SchemaURL,
		semconv.ServiceName(serviceName))

	tracerProvider := sdkTrace.NewTracerProvider(sdkTrace.WithBatcher(exp), sdkTrace.WithResource(resources))
	otel.SetTracerProvider(tracerProvider)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))

	return OpenTelemetry{
		tracerProvider: tracerProvider,
		defaultTracer:  tracerProvider.Tracer("vitess.io/vitess/go/vt/trace"),
	}, OpenTelemetryCloser{exporter: exp}, nil
}

type OpenTelemetry struct {
	tracerProvider trace.TracerProvider
	defaultTracer  trace.Tracer
}

// New creates a new span from an existing one, if provided. The parent can also be nil
func (o OpenTelemetry) New(parent Span, label string) Span {
	// TODO: This is the wrong API for Otel - I should get a context passed in, and return one
	var ctx context.Context
	if parent != nil {
		if otelSpan, ok := parent.(OtelSpan); ok {
			ctx = trace.ContextWithSpan(context.Background(), otelSpan.Span)
		}
	}
	if ctx == nil {
		ctx = context.Background()
	}
	_, span := o.defaultTracer.Start(context.Background(), label)
	return OtelSpan{span}
}

// NewFromString creates a new span and uses the provided string to reconstitute the parent span
func (o OpenTelemetry) NewFromString(parent, label string) (Span, error) {
	// TODO: This is the wrong API for Otel; I should return a context

	decodedBytes, err := base64.StdEncoding.DecodeString(parent)
	if err != nil {
		return nil, err
	}

	var data map[string]string
	err = json.Unmarshal(decodedBytes, &data)
	if err != nil {
		return nil, err
	}

	// TODO: this probably doesn't work, we need to munge the jaeger format into something more otel
	ctx := otel.GetTextMapPropagator().Extract(context.Background(), propagation.MapCarrier(data))
	_, span := o.defaultTracer.Start(ctx, label)
	return OtelSpan{span}, nil
}

// FromContext extracts a span from a context, making it possible to annotate the span with additional information
func (o OpenTelemetry) FromContext(ctx context.Context) (Span, bool) {
	span := trace.SpanFromContext(ctx)

	// SpanFromContext always returns a span, which isn't exactly what we want, so check if it's valid
	if !span.SpanContext().IsValid() {
		return nil, false
	}
	return OtelSpan{span}, true
}

// NewContext creates a new context containing the provided span
func (o OpenTelemetry) NewContext(parent context.Context, span Span) context.Context {
	otelSpan, ok := span.(OtelSpan)
	if !ok {
		return nil
	}

	return trace.ContextWithSpan(parent, otelSpan.Span)
}

func (o OpenTelemetry) AddGrpcServerOptions(_ func(s grpc.StreamServerInterceptor, u grpc.UnaryServerInterceptor), addStats func(s stats.Handler)) {
	addStats(otelgrpc.NewClientHandler(otelgrpc.WithTracerProvider(o.tracerProvider)))
}

func (o OpenTelemetry) AddGrpcClientOptions(_ func(s grpc.StreamClientInterceptor, u grpc.UnaryClientInterceptor), addStats func(s stats.Handler)) {
	addStats(otelgrpc.NewServerHandler(otelgrpc.WithTracerProvider(o.tracerProvider)))
}

type OtelSpan struct {
	trace.Span
}

func (o OtelSpan) Finish() {
	o.Span.End()
}

func (o OtelSpan) Annotate(key string, val any) {
	k := attribute.Key(key)
	var kv attribute.KeyValue

	switch v := val.(type) {
	case int:
		kv = k.Int(v)
	case int64:
		kv = k.Int64(v)
	case string:
		kv = k.String(v)
	// TODO: Are there any other types used in Vitess?
	default:
		kv = k.String(fmt.Sprint(v))
	}

	o.Span.SetAttributes(kv)
}

type OpenTelemetryCloser struct {
	exporter *otlptrace.Exporter
}

func (o OpenTelemetryCloser) Close() error {
	return o.exporter.Shutdown(context.Background())
}
