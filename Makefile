# mqlite — build / test / bench, and clean up all generated data.
# Everything under "generated data" is reproducible: clean removes it, the
# regenerate targets recreate it. Source/harness is never touched by clean.

BIN := bin

.PHONY: help build test e2e bench clean clean-docker distclean

help: ## show this help
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) \
	  | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-13s\033[0m %s\n",$$1,$$2}'

## ── regenerate ──────────────────────────────────────────────────────────────

build: ## build mqlite + mqlite-bench into bin/
	@mkdir -p $(BIN)
	go build -o $(BIN)/mqlite ./cmd/mqlite
	go build -o $(BIN)/mqlite-bench ./cmd/mqlite-bench
	@echo "built $(BIN)/mqlite, $(BIN)/mqlite-bench"

test: ## run unit + invariant tests (uses auto-cleaned temp dirs)
	go test ./...

e2e: ## run end-to-end suites against an ephemeral local broker
	./test/run.sh

bench: ## run the Docker stress matrix (regenerates bench/out/)
	./bench/run-bench.sh

## ── clean ───────────────────────────────────────────────────────────────────

clean: ## remove ALL generated/temp data (DBs, bench/out, binaries, smoke dirs)
	@echo "› generated SQLite files"
	@find . -type f \( -name '*.db' -o -name '*.db-wal' -o -name '*.db-shm' \) -print -delete
	@echo "› bench output"
	@rm -rf bench/out
	@echo "› built binaries"
	@rm -rf $(BIN) ./mqlite ./mqlite-bench
	@echo "› local smoke dirs"
	@rm -rf /tmp/mqlite-bench /tmp/benchdata /tmp/benchdata2
	@rm -f  /tmp/mqsmoke.db /tmp/mqsmoke.db-wal /tmp/mqsmoke.db-shm
	@echo "clean done — regenerate with: make build | make test | make e2e | make bench"

clean-docker: ## remove the bench container + image
	-@docker rm -f mqlite-bench-run >/dev/null 2>&1 || true
	-@docker rmi mqlite-bench:latest >/dev/null 2>&1 || true
	@echo "docker bench artifacts removed"

distclean: clean clean-docker ## clean generated data AND docker artifacts
	@echo "distclean done"
