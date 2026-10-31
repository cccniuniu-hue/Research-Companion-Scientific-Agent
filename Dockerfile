# Build from the repository; no precompiled host binary is required.
ARG GO_VERSION=1.24.13
FROM golang:${GO_VERSION}-alpine AS build
WORKDIR /src
ENV GOTOOLCHAIN=local
COPY go.mod go.sum ./
RUN go mod download
COPY config ./config
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 go build -mod=readonly -trimpath -ldflags="-s -w" -o /out/server ./cmd/server

FROM alpine:3.19
RUN apk add --no-cache ca-certificates tzdata poppler-utils     && addgroup -g 10001 app && adduser -D -u 10001 -G app app
WORKDIR /app
RUN mkdir -p config .data/artifacts && chown -R app:app /app
COPY --from=build /out/server /app/server
COPY config/config.docker.yaml /app/config/config.yaml
ENV TZ=Asia/Shanghai HOME=/app
USER 10001:10001
EXPOSE 8090
ENTRYPOINT ["/app/server"]
