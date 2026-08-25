# =============================================================================
# Oracle Property Intelligence Pipeline — Duval County, FL
# =============================================================================

# --- Configuration ---
EC2_INSTANCE    := i-00bb59df78fecdfdd
REGION          := us-east-2
CLOUDFRONT_URL  := https://d5sfa8vgu8mcx.cloudfront.net
API_URL         := $(CLOUDFRONT_URL)/api
IPNS_KEY        := k51qzi5uqu5dggq0h9xylfc0kr0kpw7i4zcacnfrymz9sjv7mpeze4femaujcz
CRM_WEBHOOK     := https://42trwtmqqe.execute-api.us-east-2.amazonaws.com/webhook/pipeline
FILEBASE_BUCKET := elephant-oracle-duval
EC2_APP_DIR     := /opt/app
TRIGGER_LIMIT   := 400000

# --- Helpers ---
define ssm_run
	@echo ">>> SSM: $(2)"
	@CMD_ID=$$(aws ssm send-command \
		--instance-ids $(EC2_INSTANCE) \
		--document-name "AWS-RunShellScript" \
		--parameters 'commands=["$(1)"]' \
		--region $(REGION) \
		--output text \
		--query "Command.CommandId") && \
	echo "CommandId: $$CMD_ID" && \
	sleep 3 && \
	aws ssm get-command-invocation \
		--command-id $$CMD_ID \
		--instance-id $(EC2_INSTANCE) \
		--region $(REGION) \
		--query "{Status:Status,Output:StandardOutputContent,Error:StandardErrorContent}" \
		--output table
endef

# =============================================================================
# Local Development
# =============================================================================

.PHONY: install
install: ## Install npm dependencies in pipeline/
	cd pipeline && npm install

.PHONY: dev
dev: ## Run pipeline locally with tsx watch
	cd pipeline && npx tsx watch src/index.ts

.PHONY: test
test: ## Run vitest tests
	cd pipeline && npx vitest run

.PHONY: test-watch
test-watch: ## Run vitest in watch mode
	cd pipeline && npx vitest

.PHONY: test-api
test-api: ## Run API integration tests
	cd pipeline && npx vitest run tests/integration/api-acceptance.test.ts --reporter=verbose

.PHONY: typecheck
typecheck: ## Run TypeScript type checking (tsc --noEmit)
	cd pipeline && npx tsc --noEmit

.PHONY: lint
lint: ## Run eslint (if configured)
	cd pipeline && npx eslint src/ --ext .ts || echo "ESLint not configured"

# =============================================================================
# Build
# =============================================================================

.PHONY: build
build: ## Build pipeline TypeScript
	cd pipeline && npx tsc

.PHONY: docker-build
docker-build: ## Build all Docker containers
	docker compose build

.PHONY: docker-up
docker-up: ## Start all services in background
	docker compose up -d

.PHONY: docker-down
docker-down: ## Stop all services
	docker compose down

.PHONY: docker-logs
docker-logs: ## Tail pipeline container logs
	docker compose logs -f pipeline

.PHONY: docker-ps
docker-ps: ## Show running containers
	docker compose ps

.PHONY: docker-restart
docker-restart: ## Restart pipeline container
	docker compose restart pipeline

# =============================================================================
# Deploy to EC2 (via SSM)
# =============================================================================

.PHONY: deploy
deploy: ec2-pull ec2-build ec2-restart ## Full deploy: pull, build, restart on EC2
	@echo "--- Deploy complete ---"

.PHONY: ec2-ssh
ec2-ssh: ## Open interactive SSM session to EC2
	aws ssm start-session --target $(EC2_INSTANCE) --region $(REGION)

.PHONY: ec2-pull
ec2-pull: ## Git pull latest code on EC2
	$(call ssm_run,cd $(EC2_APP_DIR) && git pull,pulling latest code)

.PHONY: ec2-build
ec2-build: ## Docker compose build on EC2
	$(call ssm_run,cd $(EC2_APP_DIR) && docker compose build,building docker images)

.PHONY: ec2-restart
ec2-restart: ## Docker compose down + up on EC2
	$(call ssm_run,cd $(EC2_APP_DIR) && docker compose down && docker compose up -d,restarting services)

.PHONY: ec2-logs
ec2-logs: ## Tail pipeline logs on EC2 (last 100 lines)
	$(call ssm_run,cd $(EC2_APP_DIR) && docker compose logs --tail=100 pipeline,fetching pipeline logs)

.PHONY: ec2-status
ec2-status: ## Check docker ps on EC2
	$(call ssm_run,cd $(EC2_APP_DIR) && docker compose ps,checking container status)

.PHONY: ec2-health
ec2-health: ## Check pipeline health endpoint from EC2
	$(call ssm_run,curl -s http://localhost:9080/api/health | head -200,checking health from EC2)

# =============================================================================
# Pipeline Operations
# =============================================================================

.PHONY: trigger-run
trigger-run: ## Trigger a pipeline run (default limit=200)
	@echo ">>> Triggering pipeline run (limit=$(TRIGGER_LIMIT))..."
	@curl -s -X POST "$(API_URL)/runs/trigger" \
		-H "Content-Type: application/json" \
		-d '{"limit":$(TRIGGER_LIMIT)}' | head -500

.PHONY: trigger-run-500
trigger-run-500: ## Trigger a pipeline run with limit=500
	$(MAKE) trigger-run TRIGGER_LIMIT=500

.PHONY: trigger-run-1000
trigger-run-1000: ## Trigger a pipeline run with limit=1000
	$(MAKE) trigger-run TRIGGER_LIMIT=1000

.PHONY: fetch-real-data
fetch-real-data: ## Fetch real COJ data on EC2 (needs US IP)
	$(call ssm_run,cd $(EC2_APP_DIR) && docker compose exec pipeline npx tsx src/scripts/fetch-real-data.ts,fetching real COJ data)

# =============================================================================
# Verification
# =============================================================================

.PHONY: verify-api
verify-api: ## Check /api/health endpoint
	@echo ">>> Health check..."
	@curl -s "$(API_URL)/health" | head -200
	@echo

.PHONY: verify-stats
verify-stats: ## Check /api/stats endpoint
	@echo ">>> Stats..."
	@curl -s "$(API_URL)/stats" | head -500
	@echo

.PHONY: verify-runs
verify-runs: ## Check /api/runs endpoint
	@echo ">>> Recent runs..."
	@curl -s "$(API_URL)/runs" | head -500
	@echo

.PHONY: verify-sources
verify-sources: ## Check /api/data-sources endpoint
	@echo ">>> Data sources..."
	@curl -s "$(API_URL)/data-sources" | head -500
	@echo

.PHONY: verify-ipns
verify-ipns: ## Resolve IPNS key and show index.json
	@echo ">>> Resolving IPNS: $(IPNS_KEY)..."
	@curl -s "https://dweb.link/ipns/$(IPNS_KEY)" | head -500
	@echo

.PHONY: verify-webhook
verify-webhook: ## Send test webhook to CRM (unsigned, expect 401)
	@echo ">>> Testing CRM webhook (expect 401 — unsigned)..."
	@curl -s -w "\nHTTP Status: %{http_code}\n" -X POST "$(CRM_WEBHOOK)" \
		-H "Content-Type: application/json" \
		-d '{"event":"test","timestamp":"2026-01-01T00:00:00Z"}'

.PHONY: verify
verify: verify-api verify-stats verify-runs verify-sources verify-ipns ## Run all verification checks
	@echo
	@echo "=== All verification checks complete ==="

# =============================================================================
# Help
# =============================================================================

.PHONY: help
help: ## Show all available targets
	@echo "Oracle Pipeline — Duval County, FL"
	@echo "==================================="
	@echo
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

.DEFAULT_GOAL := help
