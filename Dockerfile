FROM golang:1.26-alpine AS go_builder
WORKDIR /repo
COPY go.* ./
ENV GOPROXY=https://proxy.golang.org
RUN go mod download

COPY . ./
RUN go build -o seafile-fileserver .

FROM alpine
COPY --from=go_builder /repo/seafile-fileserver /

RUN mkdir -p /config /data /tmp /run/seafile
RUN chown 1000:1000 /config /data /tmp /run/seafile

USER 1000:1000

VOLUME /config
VOLUME /data
VOLUME /tmp

ENTRYPOINT ["/seafile-fileserver", "-F", "/config", "-d", "/data", "-p", "/run/seafile"]
