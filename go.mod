module github.com/roadrunner-server/nats/v4

go 1.21

toolchain go1.21.1

require (
	github.com/goccy/go-json v0.10.2
	github.com/google/uuid v1.3.1
	github.com/nats-io/nats.go v1.30.1
	github.com/roadrunner-server/api/v4 v4.8.0
	github.com/roadrunner-server/endure/v2 v2.4.2
	github.com/roadrunner-server/errors v1.3.0
	go.opentelemetry.io/contrib/propagators/jaeger v1.19.0
	go.opentelemetry.io/otel v1.19.0
	go.opentelemetry.io/otel/sdk v1.18.0
	go.opentelemetry.io/otel/trace v1.19.0
	go.uber.org/zap v1.26.0
)

require (
	github.com/go-logr/logr v1.2.4 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/klauspost/compress v1.17.0 // indirect
	github.com/nats-io/nats-server/v2 v2.10.1 // indirect
	github.com/nats-io/nkeys v0.4.5 // indirect
	github.com/nats-io/nuid v1.0.1 // indirect
	go.opentelemetry.io/otel/metric v1.19.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	golang.org/x/crypto v0.13.0 // indirect
	golang.org/x/sys v0.12.0 // indirect
)
