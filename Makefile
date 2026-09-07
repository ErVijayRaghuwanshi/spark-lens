.DEFAULT_GOAL := help

# Colors for terminal output
COLOR_RESET   := \033[0m
COLOR_INFO    := \033[36m
COLOR_SUCCESS := \033[32m
COLOR_WARNING := \033[33m

.PHONY: help install env dev inspector run run-sse test test-v mock-server build clean

help: ## Display this help message
	@echo "$(COLOR_INFO)SparkLens - Make Commands$(COLOR_RESET)"
	@echo "----------------------------------------------------"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  $(COLOR_SUCCESS)%-15s$(COLOR_RESET) %s\n", $$1, $$2}'

env: ## Create .env file from .env.example if not exists
	@if [ ! -f .env ]; then \
		echo "$(COLOR_INFO)Creating .env from .env.example...$(COLOR_RESET)"; \
		cp .env.example .env; \
		echo "$(COLOR_SUCCESS).env file created successfully.$(COLOR_RESET)"; \
	else \
		echo "$(COLOR_WARNING).env already exists, skipping.$(COLOR_RESET)"; \
	fi

install: env ## Install dependencies with uv
	@echo "$(COLOR_INFO)Installing dependencies with uv...$(COLOR_RESET)"
	uv sync
	@echo "$(COLOR_SUCCESS)Dependencies installed.$(COLOR_RESET)"

run: env ## Run SparkLens MCP server with STDIO transport
	@echo "$(COLOR_INFO)Starting SparkLens MCP Server (STDIO transport)...$(COLOR_RESET)"
	uv run --env-file .env python -m sparklens.server

run-sse: env ## Run SparkLens MCP server with SSE (HTTP) transport on port 8030
	@echo "$(COLOR_INFO)Starting SparkLens MCP Server (SSE transport on port 8030)...$(COLOR_RESET)"
	SPARK_MCP_TRANSPORT=sse SPARK_MCP_PORT=8030 uv run --env-file .env python -m sparklens.server

inspector: env ## Launch FastMCP Developer Inspector Web UI
	@echo "$(COLOR_INFO)Launching FastMCP Developer Inspector...$(COLOR_RESET)"
	uv run --env-file .env fastmcp dev inspector src/sparklens/server.py

dev: inspector ## Alias for inspector

test: ## Run test suite with pytest
	@echo "$(COLOR_INFO)Running test suite...$(COLOR_RESET)"
	uv run pytest

test-v: ## Run test suite with verbose output
	@echo "$(COLOR_INFO)Running test suite (verbose)...$(COLOR_RESET)"
	uv run pytest tests/ -v

mock-server: ## Run local mock Spark History Server on port 18080
	@echo "$(COLOR_INFO)Starting Mock Spark History Server on http://127.0.0.1:18080...$(COLOR_RESET)"
	python3 tests/mock_server.py 18080

build: clean ## Build distribution packages (wheel and sdist)
	@echo "$(COLOR_INFO)Building package distributions with uv...$(COLOR_RESET)"
	uv build
	@echo "$(COLOR_SUCCESS)Build complete in dist/$(COLOR_RESET)"

clean: ## Clean build artifacts, byte caches, and test caches
	@echo "$(COLOR_INFO)Cleaning temporary files and caches...$(COLOR_RESET)"
	rm -rf dist build *.egg-info .pytest_cache .coverage
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.py[cod]" -delete
	@echo "$(COLOR_SUCCESS)Clean complete.$(COLOR_RESET)"
