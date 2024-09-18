# TODO: clean this up

GO             ?= go
GO_COVER_MODE  ?= atomic
GO_COVER_FLAGS ?= -cover -covermode=$(GO_COVER_MODE)
GO_TEST_FLAGS  ?=
GO_TEST        ?= $(GO) test $(GO_COVER_FLAGS) $(GO_TEST_FLAGS)
GO_BENCH_FLAGS ?= -benchmem
GO_BENCH       ?= $(GO) test -run ^$$ -bench . $(GO_BENCH_FLAGS)

.PHONY: all
all: vet test

## Tests

.PHONY: test test_libsqlite3
test test_libsqlite3: vet
	@$(GO_TEST) ./...

# Use the installed libsqlite3 instead of the one bundled with go-sqlite3
test_libsqlite3: override GO_TEST_FLAGS += -tags=libsqlite3

## Benchmarks

.PHONY: bench benchmem bench_libsqlite3 benchmem_libsqlite3
bench benchmem bench_libsqlite3 benchmem_libsqlite3:
	@$(GO_BENCH)

# Use an in-memory database
benchmem: override GO_BENCH_FLAGS += -memory

# Use the installed libsqlite3 instead of the one bundled with go-sqlite3
bench_libsqlite3: override GO_BENCH_FLAGS += -tags=libsqlite3

# Use an in-memory database and the installed libsqlite3 instead of the one
# bundled with go-sqlite3
benchmem_libsqlite3: override GO_BENCH_FLAGS += -memory
benchmem_libsqlite3: override GO_BENCH_FLAGS += -tags=libsqlite3

.PHONY: vet
vet:
	@go vet

.PHONY: clean
clean:
	@go clean
