.PHONY: build run test docker-up docker-down lint clean

build:
	go build -ldflags="-w -s" -o bin/trending-service ./cmd/service

run:
	go run ./cmd/service

test:
	go test -race ./...

docker-up:
	docker compose -f deployments/docker-compose.yml up --build -d

docker-down:
	docker compose -f deployments/docker-compose.yml down

docker-logs:
	docker compose -f deployments/docker-compose.yml logs -f trending-service

lint:
	golangci-lint run ./...

clean:
	rm -rf bin/ trending-service
