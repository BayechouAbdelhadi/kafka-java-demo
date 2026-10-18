.DEFAULT_GOAL := help
COMPOSE := docker compose
INFRA   := kafka-1 kafka-2 kafka-3 kafka-4 schema-registry postgres kafka-ui
APPS    := order-service inventory-service notification-service

.PHONY: help infra up down logs build test demo

help: ## List targets
	@grep -E '^[a-z]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  make %-6s %s\n", $$1, $$2}'

infra: ## Start the infrastructure only (run the services from your IDE or mvn spring-boot:run)
	$(COMPOSE) up -d --wait $(INFRA)

up: ## Start the infrastructure and the three services in containers
	$(COMPOSE) --profile apps up -d --build --wait $(INFRA) $(APPS)

down: ## Stop and remove everything (no volumes: state is reset)
	$(COMPOSE) --profile apps down --remove-orphans

logs: ## Follow the services' logs
	$(COMPOSE) --profile apps logs -f $(APPS)

build: ## Build all modules
	mvn -B install -DskipTests

test: ## Run the test suite (embedded Kafka + H2, no Docker needed)
	mvn -B verify

demo: ## Run the end-to-end scenarios against running services
	./scripts/demo.sh
