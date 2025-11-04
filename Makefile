.PHONY: run-containers stop-containers build build-debug build-release test repl clean clean-all help

# Rust library configuration
RUST_FFI_DIR = iceberg_rust_ffi
BUILD_TYPE ?= debug
TARGET_DIR = $(RUST_FFI_DIR)/target/$(BUILD_TYPE)
LIB_NAME = libiceberg_rust_ffi.dylib
RUST_LIB_PATH = $(TARGET_DIR)/$(LIB_NAME)

# Julia thread configuration (only set if JULIA_NUM_THREADS is defined)
ifdef JULIA_NUM_THREADS
JULIA_THREADS_ENV = JULIA_NUM_THREADS=$(JULIA_NUM_THREADS)
else
JULIA_THREADS_ENV =
endif

# Default target
all: build

# Start docker containers
run-containers:
	cd docker && docker-compose up -d && sleep 10

# Stop docker containers
stop-containers:
	cd docker && docker-compose down

# Build the Rust FFI library (debug by default, use BUILD_TYPE=release for release build)
build:
ifeq ($(BUILD_TYPE),debug)
	cd $(RUST_FFI_DIR) && cargo build
else
	cd $(RUST_FFI_DIR) && cargo build --release
endif

# Build debug version
build-debug:
	$(MAKE) BUILD_TYPE=debug build

# Build release version
build-release:
	$(MAKE) BUILD_TYPE=release build

# Run tests (requires .env file)
test: build
	@if [ ! -f .env ]; then \
		echo "Error: .env file not found. Please create a .env file with required environment variables."; \
		exit 1; \
	fi
	@set -a && . ./.env && set +a && \
		export ICEBERG_RUST_LIB=$$(pwd)/$(TARGET_DIR) && \
		$(JULIA_THREADS_ENV) julia --project=. -e 'using Pkg; Pkg.test()'

# Start Julia REPL with environment configured (requires .env file)
repl: build
	@if [ ! -f .env ]; then \
		echo "Error: .env file not found. Please create a .env file with required environment variables."; \
		exit 1; \
	fi
	@set -a && . ./.env && set +a && \
		export ICEBERG_RUST_LIB=$$(pwd)/$(TARGET_DIR) && \
		$(JULIA_THREADS_ENV) julia --project=.

# Clean build artifacts
clean:
	$(MAKE) -C $(RUST_FFI_DIR) clean

# Clean everything
clean-all:
	$(MAKE) -C $(RUST_FFI_DIR) clean-all

# Show help
help:
	@echo "Available targets:"
	@echo "  all              - Build the Rust FFI library (default)"
	@echo "  build            - Build the Rust FFI library (use BUILD_TYPE=debug for debug build)"
	@echo "  build-debug      - Build the Rust FFI library in debug mode"
	@echo "  build-release    - Build the Rust FFI library in release mode"
	@echo "  test             - Run Julia tests (requires .env file and runs build first)"
	@echo "  repl             - Start Julia REPL with environment configured (requires .env file)"
	@echo "  run-containers   - Start docker containers"
	@echo "  stop-containers  - Stop docker containers"
	@echo "  clean            - Clean build artifacts"
	@echo "  clean-all        - Clean everything including target directory"
	@echo "  help             - Show this help message"
	@echo ""
	@echo "Examples:"
	@echo "  make test                           - Build in debug mode and run tests"
	@echo "  make BUILD_TYPE=release test        - Build in release mode and run tests"
	@echo "  make build-release repl             - Build in release mode and start REPL"
	@echo "  JULIA_NUM_THREADS=8 make test       - Run tests with 8 Julia threads"
	@echo "  JULIA_NUM_THREADS=4 make repl       - Start REPL with 4 Julia threads"