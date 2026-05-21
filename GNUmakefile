TEST?=./...
TESTARGS?=
GOFMT_FILES?=$$(find . -name '*.go' | grep -v vendor)

DOCKER_COMPOSE?=docker compose
DOCKER_COMPOSE_FILE?=docker-compose.yaml

default: build

build:
	go install

fmt:
	gofmt -w $(GOFMT_FILES)

vet:
	go vet ./...

tidy:
	go mod tidy

test:
	go test ./...

docker-up:
	$(DOCKER_COMPOSE) -f $(DOCKER_COMPOSE_FILE) up -d

docker-down:
	$(DOCKER_COMPOSE) -f $(DOCKER_COMPOSE_FILE) down

docker-logs:
	$(DOCKER_COMPOSE) -f $(DOCKER_COMPOSE_FILE) logs -f

wait-for-connect:
	@echo "Waiting for Kafka Connect to become ready..."
	@until curl -s http://localhost:8083/connectors > /dev/null; do \
		sleep 5; \
		echo "Kafka Connect not ready yet..."; \
	done
	@echo "Kafka Connect is ready."

testacc: docker-up wait-for-connect
	KAFKA_CONNECT_URL=http://localhost:8083 TF_LOG=debug TF_ACC=1 go test $(TEST) -v $(TESTARGS) -timeout 120m

testacc-clean: docker-down

clean:
	rm -rf dist/
	rm -f terraform-provider-kafka-connect

.PHONY: \
	default \
	build \
	fmt \
	vet \
	tidy \
	test \
	testacc \
	testacc-clean \
	docker-up \
	docker-down \
	docker-logs \
	wait-for-connect \
	clean