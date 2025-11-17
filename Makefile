.PHONY: run-containers stop-containers build build-debug build-release test test-dev repl repl-dev set-local-lib clear-local-lib clean clean-all help

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

# Helper target: Set local library preference for development
set-local-lib:
	@julia --project=. -e 'using Preferences; set_preferences!("iceberg_rust_ffi_jll", "libiceberg_rust_ffi_path" => "$(shell pwd)/$(TARGET_DIR)/"; force=true)'
	@echo "Set local library preference to: $(shell pwd)/$(TARGET_DIR)/"
	@echo "Julia will use this path after restarting/recompiling."

# Helper target: Clear local library preference (use JLL default)
clear-local-lib:
	@julia --project=. -e 'using Preferences; delete_preferences!("iceberg_rust_ffi_jll", "libiceberg_rust_ffi_path")'
	@echo "Cleared local library preference. Julia will use JLL artifact after restarting/recompiling."

# Run tests with local build (for development)
test-dev: build set-local-lib
	@if [ ! -f .env ]; then \
		echo "Error: .env file not found. Please create a .env file with required environment variables."; \
		exit 1; \
	fi
	@set -a && . ./.env && set +a && \
		$(JULIA_THREADS_ENV) julia --project=. -e 'using Pkg; Pkg.test()'

# Run tests with JLL package (production mode)
test: clear-local-lib
	@if [ ! -f .env ]; then \
		echo "Error: .env file not found. Please create a .env file with required environment variables."; \
		exit 1; \
	fi
	@set -a && . ./.env && set +a && \
		$(JULIA_THREADS_ENV) julia --project=. -e 'using Pkg; Pkg.test()'

# Start Julia REPL with local build (for development)
repl-dev: build set-local-lib
	@if [ ! -f .env ]; then \
		echo "Error: .env file not found. Please create a .env file with required environment variables."; \
		exit 1; \
	fi
	@set -a && . ./.env && set +a && \
		$(JULIA_THREADS_ENV) julia --project=.

# Start Julia REPL with JLL package (production mode)
repl: clear-local-lib
	@if [ ! -f .env ]; then \
		echo "Error: .env file not found. Please create a .env file with required environment variables."; \
		exit 1; \
	fi
	@set -a && . ./.env && set +a && \
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
	@echo ""
	@echo "Development targets (use local Rust build):"
	@echo "  test-dev         - Run Julia tests with local Rust library"
	@echo "  repl-dev         - Start Julia REPL with local Rust library"
	@echo "  set-local-lib    - Set preference to use local Rust library"
	@echo ""
	@echo "Production targets (use JLL package):"
	@echo "  test             - Run Julia tests with JLL package"
	@echo "  repl             - Start Julia REPL with JLL package"
	@echo "  clear-local-lib  - Clear local library preference (use JLL)"
	@echo ""
	@echo "Docker targets:"
	@echo "  run-containers   - Start docker containers"
	@echo "  stop-containers  - Stop docker containers"
	@echo ""
	@echo "Cleanup targets:"
	@echo "  clean            - Clean build artifacts"
	@echo "  clean-all        - Clean everything including target directory"
	@echo "  help             - Show this help message"
	@echo ""
	@echo "Examples:"
	@echo "  make test-dev                       - Build and test with local Rust library"
	@echo "  make BUILD_TYPE=release test-dev    - Build release and test with local library"
	@echo "  make test                           - Test with JLL package (production mode)"
	@echo "  JULIA_NUM_THREADS=8 make test-dev   - Run dev tests with 8 Julia threads"