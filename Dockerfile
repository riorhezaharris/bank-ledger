FROM golang:1.22-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o /bank-ledger-node ./cmd/node

FROM alpine:3.19
RUN apk add --no-cache ca-certificates iptables
COPY --from=builder /bank-ledger-node /bank-ledger-node
ENTRYPOINT ["/bank-ledger-node"]
