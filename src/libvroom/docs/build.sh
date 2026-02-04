#!/bin/bash
# Build documentation: Doxygen -> doxybook2 -> quartodoc -> Quarto
set -e

cd "$(dirname "$0")"

echo "==> Running Doxygen (XML only)..."
rm -rf api
doxygen Doxyfile

echo "==> Running doxybook2..."
rm -rf api-reference
mkdir -p api-reference
doxybook2 --input api/xml --output api-reference --config .doxybook/config.json

# Create index.qmd for API reference landing page
cat > api-reference/index.qmd << 'EOF'
---
title: "C++ API Reference"
---

Complete C++ API documentation for libvroom, generated from source code comments.

## Quick Links

### Core Classes

| Class | Description |
|-------|-------------|
| [Parser](/api-reference/Classes/classlibvroom_1_1Parser.qmd) | Main CSV parser class with unified API |
| [FileBuffer](/api-reference/Classes/classlibvroom_1_1FileBuffer.qmd) | RAII wrapper for file buffers with automatic cleanup |
| [ParseIndex](/api-reference/Classes/classlibvroom_1_1ParseIndex.qmd) | Result structure containing parsed field positions |
| [TwoPass](/api-reference/Classes/classlibvroom_1_1TwoPass.qmd) | Low-level two-pass parsing algorithm |

### Configuration

| Type | Description |
|------|-------------|
| [ParseOptions](/api-reference/Classes/structlibvroom_1_1ParseOptions.qmd) | Configuration options for parsing |
| [SizeLimits](/api-reference/Classes/structlibvroom_1_1SizeLimits.qmd) | Size limits for secure CSV parsing |

### Error Handling

| Type | Description |
|------|-------------|
| [ErrorCollector](/api-reference/Classes/classlibvroom_1_1ErrorCollector.qmd) | Collects and manages parse errors |
| [ParseError](/api-reference/Classes/structlibvroom_1_1ParseError.qmd) | Structure representing a single parse error |
| [ParseException](/api-reference/Classes/classlibvroom_1_1ParseException.qmd) | Exception thrown in STRICT error mode |

## Full Reference

- [All Classes](index_classes.qmd)
- [All Files](index_files.qmd)
- [Namespaces](index_namespaces.qmd)
- [Examples](index_examples.qmd)
EOF

echo "==> Running quartodoc (Python API)..."
rm -rf python-reference
quartodoc build

echo "==> Running Quarto..."
quarto render

echo "==> Done! Output in _site/"
