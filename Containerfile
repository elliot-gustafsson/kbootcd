FROM docker.io/golang:1.26.3 AS builder

WORKDIR /workspace

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build

FROM docker.io/alpine:3.22@sha256:2039be0c5ec6ce8566809626a252c930216a92109c043f282504accb5ee3c0c6

COPY --from=builder /workspace/kbootcd /usr/local/bin/kbootcd

USER root
ENTRYPOINT ["/usr/local/bin/kbootcd"]
