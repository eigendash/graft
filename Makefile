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
# The host may be shared. Every benchmark is run BENCHCOUNT times and the
# results file keeps all of the runs, so a reader can take the least-contended
# one rather than trusting a single sample.
BENCHCOUNT  ?= 5
SEEDS       ?= 200
LINSEEDS    ?= 60
ELECTIONS   ?= 200

.PHONY: all build vet fmt fmt-check test test-race test-count model-check bench results \
        prove-regressions verify clean help

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
	@echo "  prove-regressions  show that each regression test fails against the code"
	@echo "                     it was written for ($(RESULTS)/regressions.txt)"
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

# Results files record the command that produced them in a portable form. The
# toolchain location is this workspace's business, not the artefact's: echoing
# the real recipe would bake an absolute path containing a local username into
# a file that is meant to be published. The environment that actually matters to
# a reader -- Go version, GOOS/GOARCH, CPU count -- is recorded by
# cmd/graftbench in results/graftbench.txt instead.
BENCH_CMD  := go test -run XXX -bench=. -benchtime=$(BENCHTIME) -count=$(BENCHCOUNT) ./...
RACE_CMD   := go test -race -count=1 -timeout 1800s -v ./...
SWEEP_CMD  := go run ./cmd/graftbench -seeds $(SEEDS) -linseeds $(LINSEEDS) -elections $(ELECTIONS)

bench:
	@echo "$$ $(BENCH_CMD)"
	@echo "$$ uptime: $$(uptime)"
	@$(GO) test -run XXX -bench=. -benchtime=$(BENCHTIME) -count=$(BENCHCOUNT) ./...

# Reverts each fix, runs the test that pins it, records the failure, restores
# the source and shows the test passing again. A regression test that has never
# been seen to fail is a comment.
prove-regressions:
	@sh scripts/prove-regressions.sh $(RESULTS)/regressions.txt

# Everything the README quotes comes from a file this target writes.
results:
	@mkdir -p $(RESULTS)
	@$(GO) vet ./... > $(RESULTS)/vet.txt 2>&1; echo "go vet ./... exit=$$?" >> $(RESULTS)/vet.txt
	@{ echo "$$ $(RACE_CMD)"; echo "$$ uptime: $$(uptime)"; echo; $(GO) test -race -count=1 -timeout 1800s -v ./... ; } > $(RESULTS)/tests.txt 2>&1
	@{ echo "$$ $(BENCH_CMD)"; echo "$$ uptime: $$(uptime)"; echo; $(GO) test -run XXX -bench=. -benchtime=$(BENCHTIME) -count=$(BENCHCOUNT) ./... ; } > $(RESULTS)/bench.txt 2>&1
	@$(GO) run ./cmd/graftbench -seeds $(SEEDS) -linseeds $(LINSEEDS) -elections $(ELECTIONS) > /dev/null
	@sh scripts/prove-regressions.sh $(RESULTS)/regressions.txt > /dev/null
	@echo "wrote $(RESULTS)/{vet,tests,bench,graftbench,regressions,timing-bisection}.txt and $(RESULTS)/summary.json"

verify: vet fmt-check test-race results prove-regressions

clean:
	rm -f $(RESULTS)/*.txt $(RESULTS)/*.json
