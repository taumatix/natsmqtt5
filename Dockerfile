# syntax=docker/dockerfile:1

# The builder always runs on the machine doing the building and cross-compiles
# to the target, because the broker is pure Go with CGO off. That keeps a
# multi-architecture build to one native compile per architecture instead of a
# QEMU-emulated one.
FROM --platform=$BUILDPLATFORM golang:1.25-alpine AS build

WORKDIR /src

# Dependencies resolve in their own layer, so editing the source does not
# re-download the module graph.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG TARGETOS
ARG TARGETARCH
# VERSION is what `natsmqtt5 -version` reports. The release workflow passes the
# git tag; a local build says "dev".
ARG VERSION=dev
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
	go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" \
	-o /out/natsmqtt5 ./cmd/natsmqtt5

# distroless/static carries CA certificates, which a NATS connection over TLS
# needs, and nothing else — no shell, no package manager, and a non-root user.
FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /out/natsmqtt5 /usr/local/bin/natsmqtt5

# The image behaves exactly like the binary: :1883 for MQTT, and a NATS URL of
# nats://127.0.0.1:4222 that anything real has to override, because inside a
# container localhost is the container itself. compose.yaml sets it.
EXPOSE 1883
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/natsmqtt5"]
