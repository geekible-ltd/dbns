# NDB workspace Makefile
#
#   make dev     — build + run ndb-server AND the UI dev server together
#   make server  — run ndb-server only (config: ndb.yaml, REST :8080, gRPC :9090)
#   make ui      — run the Vite dev server only (http://localhost:5173)
#   make build   — cargo build + UI production build (ui/dist)
#   make test    — full workspace test suite
#   make check   — clippy -D warnings + fmt --check
#   make fmt     — cargo fmt
#   make clean   — cargo clean + UI artifacts
#   make clean-data — also delete ./data (the database files)
#
# Auth: unset = open access (dev default). Set NDB_AUTH_READER_TOKEN /
# NDB_AUTH_WRITER_TOKEN / NDB_AUTH_ADMIN_TOKEN to require bearer tokens.

CONFIG ?= ndb.yaml
CARGO  ?= cargo
NPM    ?= npm

.PHONY: help dev server ui build test check fmt clean clean-data setup

help: ## List targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | \
	  awk 'BEGIN {FS = ":.*?## "}; {printf "  make %-12s %s\n", $$1, $$2}'

ui/node_modules: ui/package.json ui/package-lock.json
	cd ui && $(NPM) install
	@touch $@

setup: ui/node_modules ## Install UI dependencies
	$(CARGO) fetch

build: ui/node_modules ## Build server + UI
	$(CARGO) build --workspace
	cd ui && $(NPM) run build

dev: ui/node_modules ## Run server + UI dev server together (Ctrl-C stops both)
	$(CARGO) build -p ndb-server
	@trap 'kill $$(jobs -p) 2>/dev/null' INT TERM; \
	  echo "ndb-server: REST http://localhost:8080/api/v1  gRPC :9090"; \
	  echo "ui:         http://localhost:5173"; \
	  ./target/debug/ndb-server --config $(CONFIG) & \
	  (cd ui && $(NPM) run dev) & \
	  wait

server: ## Run ndb-server only
	$(CARGO) run -p ndb-server -- --config $(CONFIG)

ui: ui/node_modules ## Run the UI dev server only
	cd ui && $(NPM) run dev

test: ## Run the full workspace test suite
	$(CARGO) test --workspace

check: ## clippy -D warnings + fmt --check
	$(CARGO) clippy --workspace --all-targets -- -D warnings
	$(CARGO) fmt --all -- --check

fmt: ## Format the workspace
	$(CARGO) fmt --all

clean: ## Remove build artifacts
	$(CARGO) clean
	rm -rf ui/dist ui/node_modules ui/tsconfig.tsbuildinfo

clean-data: clean ## Also delete the database files in ./data
	rm -rf data
