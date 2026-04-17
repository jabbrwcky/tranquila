package telemetry

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	otelprom "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

const meterName = "tranquila"

type Config struct {
	Exporter     string // "prometheus", "otlp", "none"
	Addr         string // prometheus listen address
	OTLPEndpoint string
}

type Provider struct {
	Meter      metric.Meter
	meterProv  *sdkmetric.MeterProvider
	httpServer *http.Server
}

func Setup(ctx context.Context, cfg Config) (*Provider, error) {
	var (
		mp         *sdkmetric.MeterProvider
		httpServer *http.Server
	)

	switch cfg.Exporter {
	case "prometheus":
		exp, err := otelprom.New()
		if err != nil {
			return nil, fmt.Errorf("create prometheus exporter: %w", err)
		}
		mp = sdkmetric.NewMeterProvider(sdkmetric.WithReader(exp))

		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())
		httpServer = &http.Server{Addr: cfg.Addr, Handler: mux}
		go func() {
			if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				otel.Handle(fmt.Errorf("prometheus http server: %w", err))
			}
		}()

	case "otlp":
		if cfg.OTLPEndpoint == "" {
			return nil, fmt.Errorf("otlp exporter requires --telemetry-otlp-endpoint")
		}
		exp, err := otlpmetricgrpc.New(ctx,
			otlpmetricgrpc.WithEndpoint(cfg.OTLPEndpoint),
			otlpmetricgrpc.WithInsecure(),
		)
		if err != nil {
			return nil, fmt.Errorf("create otlp exporter: %w", err)
		}
		mp = sdkmetric.NewMeterProvider(
			sdkmetric.WithReader(sdkmetric.NewPeriodicReader(exp,
				sdkmetric.WithInterval(15*time.Second),
			)),
		)

	case "none", "":
		mp = sdkmetric.NewMeterProvider()

	default:
		return nil, fmt.Errorf("unknown telemetry exporter %q (want: prometheus, otlp, none)", cfg.Exporter)
	}

	otel.SetMeterProvider(mp)

	return &Provider{
		Meter:      mp.Meter(meterName),
		meterProv:  mp,
		httpServer: httpServer,
	}, nil
}

func (p *Provider) Shutdown(ctx context.Context) {
	if p.httpServer != nil {
		_ = p.httpServer.Shutdown(ctx)
	}
	_ = p.meterProv.Shutdown(ctx)
}
