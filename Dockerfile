FROM golang:1.26-alpine AS go_builder
WORKDIR /repo
COPY go.* ./
ENV GOPROXY=https://proxy.golang.org
RUN go mod download

COPY . ./
RUN go build -o notification-server .

FROM alpine
COPY --from=go_builder /repo/notification-server /
USER 1000:1000

ENTRYPOINT ["/notification-server", "-c", "/config"]
