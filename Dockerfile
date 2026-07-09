FROM golang:1.26-alpine AS builder
WORKDIR /app
COPY . .
RUN go build -o node-server ./node-server/

FROM alpine:latest
WORKDIR /app
RUN apk add --no-cache iproute2 fio
RUN mkdir -p /wal
COPY --from=builder /app/node-server .
EXPOSE 50051 50052 50053 50054 50055
ENTRYPOINT ["./node-server"]