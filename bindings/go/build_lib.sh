#!/bin/bash
# bindings/go/build_lib.sh

set -e

echo "Building Limbo Go library for current platform..."

# Determine platform-specific details
case "$(uname -s)" in
    Darwin*)
        OUTPUT_NAME="lib_limbo_go.dylib"
        # Map x86_64 to amd64 for Go compatibility
        ARCH=$(uname -m)
        if [ "$ARCH" == "x86_64" ]; then
            ARCH="amd64"
        fi
        PLATFORM="darwin_${ARCH}"
        ;;
    Linux*)
        OUTPUT_NAME="lib_limbo_go.so"
        # Map x86_64 to amd64 for Go compatibility
        ARCH=$(uname -m)
        if [ "$ARCH" == "x86_64" ]; then
            ARCH="amd64"
        fi
        PLATFORM="linux_${ARCH}"
        ;;
    MINGW*|MSYS*|CYGWIN*)
        OUTPUT_NAME="lib_limbo_go.dll"
        if [ "$(uname -m)" == "x86_64" ]; then
            PLATFORM="windows_amd64"
        else
            PLATFORM="windows_386"
        fi
        ;;
    *)
        echo "Unsupported platform: $(uname -s)"
        exit 1
        ;;
esac

# Create output directory
OUTPUT_DIR="libs/${PLATFORM}"
mkdir -p "$OUTPUT_DIR"

# Build the library
cargo build --package limbo-go

# Copy to the appropriate directory
echo "Copying $OUTPUT_NAME to $OUTPUT_DIR/"
cp "../../target/debug/$OUTPUT_NAME" "$OUTPUT_DIR/"

echo "Library built successfully for $PLATFORM"
