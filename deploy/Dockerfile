# syntax=docker/dockerfile:1
FROM docker.io/library/golang:1.27-alpine AS build
WORKDIR /src
ENV CGO_ENABLED=0 GOFLAGS=-trimpath
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -ldflags="-s -w" -o /out/webhookd ./cmd/webhookd \
 && go build -ldflags="-s -w" -o /out/mockreceiver ./cmd/mockreceiver

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/webhookd /webhookd
COPY --from=build /out/mockreceiver /mockreceiver
USER nonroot:nonroot
EXPOSE 8080 9090
ENTRYPOINT ["/webhookd"]
