.PHONY: deps build test test-integration up down logs demo-partition demo-heal demo-write health

deps:
	go mod tidy

build:
	go build ./...

test:
	go test ./...

# Requires a running cluster: make up
test-integration:
	go test -tags integration -v -count=1 ./test/integration/

up:
	docker compose up --build -d
	@echo "Nodes:"
	@echo "  node1 → http://localhost:8081"
	@echo "  node2 → http://localhost:8082"
	@echo "  node3 → http://localhost:8083"

down:
	docker compose down -v

logs:
	docker compose logs -f node1 node2 node3

# Simulate minority partition: drop node3's cluster traffic to node1/node2.
# Uses iptables inside node3's network namespace so port-publishing (8083)
# stays intact — you can still query node3 and observe the 503 responses.
# After ~1.5s, node3 flips canWrite=false and rejects writes.
demo-partition:
	$(eval N1 := $(shell docker inspect bank-ledger-node1-1 --format '{{(index .NetworkSettings.Networks "bank-ledger_cluster").IPAddress}}'))
	$(eval N2 := $(shell docker inspect bank-ledger-node2-1 --format '{{(index .NetworkSettings.Networks "bank-ledger_cluster").IPAddress}}'))
	@echo "Blocking node3 → node1 ($(N1)) and node3 → node2 ($(N2))..."
	docker exec bank-ledger-node3-1 iptables -A OUTPUT -d $(N1) -j DROP
	docker exec bank-ledger-node3-1 iptables -A OUTPUT -d $(N2) -j DROP
	docker exec bank-ledger-node3-1 iptables -A INPUT  -s $(N1) -j DROP
	docker exec bank-ledger-node3-1 iptables -A INPUT  -s $(N2) -j DROP
	@echo "node3 is isolated. Wait ~1.5s, then:"
	@echo "  make demo-write             → writes to node1/node2 succeed"
	@echo "  curl localhost:8083/health  → {\"can_write\":false}"

demo-heal:
	$(eval N1 := $(shell docker inspect bank-ledger-node1-1 --format '{{(index .NetworkSettings.Networks "bank-ledger_cluster").IPAddress}}'))
	$(eval N2 := $(shell docker inspect bank-ledger-node2-1 --format '{{(index .NetworkSettings.Networks "bank-ledger_cluster").IPAddress}}'))
	@echo "Restoring node3 cluster traffic..."
	-docker exec bank-ledger-node3-1 iptables -D OUTPUT -d $(N1) -j DROP
	-docker exec bank-ledger-node3-1 iptables -D OUTPUT -d $(N2) -j DROP
	-docker exec bank-ledger-node3-1 iptables -D INPUT  -s $(N1) -j DROP
	-docker exec bank-ledger-node3-1 iptables -D INPUT  -s $(N2) -j DROP
	@echo "node3 will resync and rejoin within one heartbeat interval (~500ms)."

# Fire a test transfer to node1 (majority partition).
demo-write:
	@curl -s -X POST http://localhost:8081/transfers \
	  -H "Content-Type: application/json" \
	  -d '{"idempotency_key":"demo-1","from_account_id":"$(FROM)","to_account_id":"$(TO)","amount_cents":1000,"currency":"USD"}' | jq .

health:
	@echo "=== node1 ===" && curl -s http://localhost:8081/health | jq .
	@echo "=== node2 ===" && curl -s http://localhost:8082/health | jq .
	@echo "=== node3 ===" && curl -s http://localhost:8083/health | jq .
