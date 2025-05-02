#!/bin/bash
# bindings/go/build_lib.sh

set -e

echo "Building Limbo Go library for current platform..."

# Determine platform-specific details
case "$(uname -s)" in
    Darwin*)
        OUTPUT_NAME="lib_limbo_go.dylib"
        PLATFORM="darwin_$(uname -m)"
        ;;
    Linux*)
        OUTPUT_NAME="lib_limbo_go.so"
        PLATFORM="linux_$(uname -m)"
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
cargo build --release --package limbo-go

# Copy to the appropriate directory
echo "Copying $OUTPUT_NAME to $OUTPUT_DIR/"
cp "../../target/release/$OUTPUT_NAME" "$OUTPUT_DIR/"

echo "Library built successfully for $PLATFORM"
