# Cross-compiles on the build machine, so multi-arch builds need no emulation.
FROM --platform=$BUILDPLATFORM golang:1.25-alpine AS build
ARG TARGETOS TARGETARCH
WORKDIR /src
COPY . .
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -ldflags=-s -o /weir-server .

FROM alpine:3.20
COPY --from=build /weir-server /usr/local/bin/weir-server
# Any non-root user works; a numeric one lets Kubernetes check runAsNonRoot.
USER 65532:65532
EXPOSE 4100
ENTRYPOINT ["weir-server"]
