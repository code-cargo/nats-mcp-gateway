# natsmcp — the NATS MCP gateway as a container.
#
# The image is a single static binary on distroless: ~20MB, no shell, no
# package manager. It serves three roles:
#   - the central gateway deployment (fronts HTTP + local stdio MCP servers);
#   - a scoped per-user pod's entrypoint (see README "Local and remote
#     servers") — though curated server images typically COPY the binary out
#     of this image instead of building FROM it, since they need their own
#     runtime (Python/Node) anyway:
#       COPY --from=ghcr.io/code-cargo/natsmcp:v1.2.3 /usr/local/bin/natsmcp /usr/local/bin/natsmcp
#   - the shim/call CLIs for debugging inside a cluster.
#
# Cross-compilation happens in the build stage on the BUILD platform (Go
# needs no emulation), so multi-arch builds are fast: TARGETOS/TARGETARCH
# are supplied by buildx per requested platform.

FROM --platform=$BUILDPLATFORM golang:1.25 AS build
WORKDIR /src

# Module downloads cache independently of source changes.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
ARG TARGETOS TARGETARCH
ARG VERSION=dev
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -ldflags="-w -s -X main.version=$VERSION" -o /out/natsmcp .

# distroless/static (not scratch): ships CA certificates (TLS to NATS) and a
# writable /tmp (the stdio backend's per-process workdir). nonroot because
# nothing here needs root.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/natsmcp /usr/local/bin/natsmcp
ENTRYPOINT ["/usr/local/bin/natsmcp"]
CMD ["--help"]
