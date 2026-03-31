# Stage 1: Build
FROM golang:1.23-alpine AS builder

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /mammoth ./cmd/mammoth

# Stage 2: Runtime
FROM scratch

COPY --from=builder /mammoth /mammoth
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

EXPOSE 27017 8081 9100

VOLUME ["/data"]

ENTRYPOINT ["/mammoth"]
CMD ["serve"]
