FROM golang:alpine AS builder
WORKDIR /build
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o api *.go

FROM alpine:latest
WORKDIR /app
COPY --from=builder /build/api .
EXPOSE 8000
CMD ["./api"]