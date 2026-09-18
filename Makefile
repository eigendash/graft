# graft — Raft with deterministic fault-injection simulation.
#
# The Go toolchain lives outside the module in this workspace, so every target
# exports it explicitly rather than relying on the ambient PATH. Override
# GOROOT/GOCACHE/GOMODCACHE/GOPATH on the command line to use another toolchain.

GOROOT      ?= $(CURDIR)/../.toolchain/go
GOCACHE     ?= $(CURDIR)/../.gocache
GOMODCACHE  ?= $(CURDIR)/../.gomodcache
GOPATH      ?= $(CURDIR)/../.gopath
GO          := PATH="$(GOROOT)/bin:$$PATH" GOROOT="$(GOROOT)" GOCACHE="$(GOCACHE)" GOMODCACHE="$(GOMODCACHE)" GOPATH="$(GOPATH)" go
GOFMT       := "$(GOROOT)/bin/gofmt"

RESULTS     := results
BENCHTIME   ?= 1s
SEEDS       ?= 200
LINSEEDS    ?= 60
ELECTIONS   ?= 200

.PHONY: all build vet fmt fmt-check test test-race test-count model-check bench results verify clean help

help:
	@echo "graft targets:"
	@echo "  build        compile every package"
	@echo "  vet          go vet ./..."
	@echo "  fmt          gofmt -w ."
	@echo "  fmt-check    fail if any file is not gofmt'd"
	@echo "  test         go test ./..."
	@echo "  test-race    go test -race -count=1 ./...          (the required check)"
	@echo "  test-count   go test -race -count=3 ./raft ./kv ./lcheck"
	@echo "  model-check  run the deterministic simulator sweeps (cmd/graftbench)"
	@echo "  bench        go test -run XXX -bench=. -benchtime=$(BENCHTIME) ./..."
	@echo "  results      regenerate everything under $(RESULTS)/"
	@echo "  verify       vet + test-race + bench + model-check, all clean"

all: build

build:
	$(GO) build ./...

vet:
	$(GO) vet ./...

fmt:
	$(GOFMT) -w .

fmt-check:
	@out=$$($(GOFMT) -l .); if [ -n "$$out" ]; then echo "not gofmt'd:"; echo "$$out"; exit 1; fi

test:
	$(GO) test ./...

test-race:
	$(GO) test -race -count=1 -timeout 1800s ./...

test-count:
	$(GO) test -race -count=3 -timeout 1800s ./raft/... ./kv/... ./lcheck/...

# The model check is the project's central verification claim: hundreds of
# seeds, each with random partitions, drops, duplicates, reordering, crashes
# and snapshot installs, with the safety invariants checked after every tick.
model-check:
	$(GO) run ./cmd/graftbench -seeds $(SEEDS) -linseeds $(LINSEEDS) -elections $(ELECTIONS)

bench:
	$(GO) test -run XXX -bench=. -benchtime=$(BENCHTIME) -count=1 ./...

# Everything the README quotes comes from a file this target writes.
results:
	@mkdir -p $(RESULTS)
	@$(GO) vet ./... > $(RESULTS)/vet.txt 2>&1; echo "go vet ./... exit=$$?" >> $(RESULTS)/vet.txt
	$(GO) test -race -count=1 -timeout 1800s -v ./... > $(RESULTS)/tests.txt 2>&1
	$(GO) test -run XXX -bench=. -benchtime=$(BENCHTIME) -count=1 ./... > $(RESULTS)/bench.txt 2>&1
	$(GO) run ./cmd/graftbench -seeds $(SEEDS) -linseeds $(LINSEEDS) -elections $(ELECTIONS) > /dev/null
	@echo "wrote $(RESULTS)/{vet,tests,bench,graftbench}.txt and $(RESULTS)/summary.json"

verify: vet fmt-check test-race results

clean:
	rm -f $(RESULTS)/*.txt $(RESULTS)/*.json
