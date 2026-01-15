#!/bin/bash
# Build documentation: Doxygen -> doxybook2 -> Quarto
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
title: "API Reference"
---

Complete API documentation for libvroom, generated from source code comments.

## Quick Links

### Core Classes

| Class | Description |
|-------|-------------|
| [two_pass](/api-reference/Classes/classlibvroom_1_1two__pass.qmd) | Main CSV parser using speculative two-pass algorithm |
| [index](/api-reference/Classes/classlibvroom_1_1index.qmd) | Result structure containing parsed field positions |
| [ErrorCollector](/api-reference/Classes/classlibvroom_1_1ErrorCollector.qmd) | Collects and manages parse errors |
| [parser](/api-reference/Classes/classlibvroom_1_1parser.qmd) | Convenience wrapper for parsing operations |

### Error Handling

| Type | Description |
|------|-------------|
| [ParseError](/api-reference/Classes/structlibvroom_1_1ParseError.qmd) | Structure representing a single parse error |
| [ParseException](/api-reference/Classes/classlibvroom_1_1ParseException.qmd) | Exception thrown in STRICT error mode |

## Full Reference

- [All Classes](index_classes.qmd)
- [All Files](index_files.qmd)
- [Namespaces](index_namespaces.qmd)
- [Examples](index_examples.qmd)
EOF

echo "==> Running Quarto..."
quarto render

echo "==> Done! Output in _site/"
