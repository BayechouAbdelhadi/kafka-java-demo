.DEFAULT_GOAL := help
COMPOSE := docker compose
INFRA   := kafka-1 kafka-2 kafka-3 kafka-4 schema-registry postgres kafka-ui

.PHONY: help start infra up down logs build test demo ui

help: ## List targets
	@grep -E '^[a-z]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  make %-6s %s\n", $$1, $$2}'

start: ## Start everything in one command: infrastructure, services, then the scenario UI
	$(MAKE) up
	$(MAKE) ui

infra: ## Start the infrastructure only, in Docker (then run the services from your IDE)
	$(COMPOSE) up -d --wait $(INFRA)

up: infra build ## Start the infrastructure, then the three services as local JVMs (logs in logs/)
	./scripts/services.sh start

down: ## Stop the services and remove the infrastructure (no volumes: state is reset)
	./scripts/services.sh stop
	$(COMPOSE) down --remove-orphans

logs: ## Follow the services' logs
	tail -F logs/*.log

build: ## Build all modules
	mvn -B install -DskipTests

test: ## Run the test suite (embedded Kafka + the compose PostgreSQL, *_test databases)
	$(COMPOSE) up -d --wait postgres
	mvn -B verify

demo: ## Run the end-to-end scenarios against running services
	./scripts/demo.sh

ui: ## Open the scenario UI on http://localhost:5173 (needs Node; services must be running)
	cd scenario-ui && npm install --no-audit --no-fund && npm run dev
