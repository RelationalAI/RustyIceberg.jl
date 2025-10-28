# Makefile for Iceberg C API integration test

# Variables
LIB_NAME = libiceberg_rust_ffi.dylib
TARGET_DIR = target/release
HEADER_DIR = include

# Compiler and flags
CC = gcc
CFLAGS = -Wall -Wextra -std=c99 -I$(HEADER_DIR)
LDFLAGS = -ldl -lm

TARGET = local

# Default target
all: build

# Build the Rust library
build:
	@if [ "$(TARGET)" = "local" ]; then \
		cargo build --release --no-default-features; \
	else \
		cargo build --release --no-default-features --target $(TARGET); \
	fi

# Clean build artifacts
clean:
	cargo clean
	rm -f $(LIB_NAME)

# Clean everything including target
clean-all: clean
	rm -rf target/

# Show help
help:
	@echo "Available targets:"
	@echo "  all             - Build everything"
	@echo "  build           - Build everything"
	@echo "  clean           - Clean build artifacts"
	@echo "  clean-all       - Clean everything including target directory"
	@echo "  help            - Show this help message"

.PHONY: all build clean clean-all help