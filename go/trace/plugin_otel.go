package trace

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"strings"

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

// NewSpan starts a span using the default otel tracer
func (o OpenTelemetry) NewSpan(parent context.Context, label string) (Span, context.Context) {
	ctx, span := o.defaultTracer.Start(parent, label)
	return OtelSpan{span}, ctx
}

// parseUberTraceID parses the legacy Uber Trace IDs that Vitess is documented to work with
// This is here as a transition mechanism.
func parseUberTraceID(value string) (trace.TraceID, trace.SpanID, error) {
	split := strings.SplitN(value, ":", 5)
	if len(split) != 4 {
		return trace.TraceID{}, trace.SpanID{}, fmt.Errorf("invalid uber-trace-id: %d parts", len(split))
	}

	// split[2] is deprecated and ignored
	// TODO: split[3] is flags
	traceID, spanID := split[0], split[1]

	if len(traceID) < 32 {
		// Receivers MUST accept hex-strings shorter than 32 characters and 0-pad them on the left
		traceID = strings.Repeat("0", 32-len(traceID)) + traceID
	}

	tID, err := trace.TraceIDFromHex(traceID)
	if err != nil {
		return trace.TraceID{}, trace.SpanID{}, err
	}

	sID, err := trace.SpanIDFromHex(spanID)
	if err != nil {
		return trace.TraceID{}, trace.SpanID{}, err
	}

	return tID, sID, nil
}

// NewFromString creates a new span and uses the provided string to reconstitute the parent span
func (o OpenTelemetry) NewFromString(inCtx context.Context, parent, label string) (Span, context.Context, error) {
	haxSpan, _ := o.NewSpan(inCtx, "decoding incoming label")
	defer haxSpan.Finish()

	haxSpan.Annotate("label", label)
	haxSpan.Annotate("parent string", parent)

	decodedBytes, err := base64.StdEncoding.DecodeString(parent)
	if err != nil {
		return nil, nil, err
	}

	haxSpan.Annotate("decoded", decodedBytes)

	var data map[string]string
	err = json.Unmarshal(decodedBytes, &data)
	if err != nil {
		return nil, nil, err
	}

	sc := trace.SpanContext{}
	if uberTraceID, ok := data["uber-trace-id"]; ok {
		tID, sID, err := parseUberTraceID(uberTraceID)
		if err != nil {
			haxSpan.Annotate("uber-trace-id", err.Error())
			return nil, nil, err
		}
		sc = sc.WithTraceID(tID).WithSpanID(sID)
		haxSpan.Annotate("parsed-trace-id", tID.String())
		haxSpan.Annotate("parsed-span-id", sID.String())
	}

	ctx, span := o.defaultTracer.Start(trace.ContextWithRemoteSpanContext(inCtx, sc), label, trace.WithSpanKind(trace.SpanKindServer))
	span.SetAttributes(attribute.String("mattm-test", "serverspan"))
	return OtelSpan{span}, ctx, nil
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
