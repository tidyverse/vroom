# libvroom Production Readiness Plan

## Project Status

libvroom is a high-performance CSV parser using portable SIMD instructions via Google Highway. The core parsing functionality is implemented and tested across x86-64 (SSE/AVX2) and ARM64 (NEON) architectures.

### Completed

- **Portable SIMD parsing** via Google Highway 1.3.0
- **Multi-threaded two-pass algorithm** based on Chang et al. (SIGMOD 2019)
- **Comprehensive error handling** with STRICT/PERMISSIVE/BEST_EFFORT modes (16 error types)
- **Dialect detection** with CleverCSV-inspired algorithm (delimiter, quote, escape, line ending detection)
- **309 tests** covering well-formed CSVs, malformed inputs, edge cases, and dialect detection
- **CI/CD** with GitHub Actions (Ubuntu, macOS, x86, ARM), Codecov integration
- **Documentation site** with Quarto and Doxygen API reference
- **Benchmark suite** with Google Benchmark, comparing against DuckDB, zsv, Arrow
- **CLI utility** (`scsv`) for CSV parsing and row counting

### Remaining Work

The remaining work is tracked as GitHub issues:

| Category | Issue | Description |
|----------|-------|-------------|
| API | [#26](https://github.com/jimhester/libvroom/issues/26) | Simplify public API |
| API | [#35](https://github.com/jimhester/libvroom/issues/35) | C vs C++ API design |
| Features | [#27](https://github.com/jimhester/libvroom/issues/27) | Streaming API (chunked parsing) |
| Features | [#29](https://github.com/jimhester/libvroom/issues/29) | Apache Arrow output |
| Features | [#37](https://github.com/jimhester/libvroom/issues/37) | Field type detection |
| Features | [#38](https://github.com/jimhester/libvroom/issues/38) | Value extraction |
| Testing | [#31](https://github.com/jimhester/libvroom/issues/31) | Fuzz testing |
| Testing | [#32](https://github.com/jimhester/libvroom/issues/32) | Verify tests check behavior, not just non-failure |
| Performance | [#28](https://github.com/jimhester/libvroom/issues/28) | Benchmark comparison CI job |
| Performance | [#33](https://github.com/jimhester/libvroom/issues/33) | Regression detection |
| Research | [#34](https://github.com/jimhester/libvroom/issues/34) | Review literature review for accuracy |
| Developer | [#36](https://github.com/jimhester/libvroom/issues/36) | Debug mode |

## Primary Goal

Integrate libvroom as the parsing backend for R's [vroom](https://github.com/tidyverse/vroom) package, targeting 2-3x faster indexing than the current implementation.

### vroom Integration Tasks (Not Yet GitHub Issues)

These tasks are specific to vroom integration and may be created as issues when work begins:

- **cpp11 bindings** for R integration
- **vroom-compatible index format** matching vroom's internal representation
- **Altrep integration** for lazy value materialization
- **Progress reporting** integration with R's progress API

## Architecture Overview

See the [Architecture documentation](https://jimhester.github.io/libvroom/architecture.html) for details on the two-pass algorithm.

## References

- Chang et al., "Speculative Distributed CSV Data Parsing", SIGMOD 2019
- Langdale & Lemire, "Parsing Gigabytes of JSON per Second", VLDB Journal 2019
- van den Burgh & Nazabal, "CleverCSV", arXiv:1811.11242, 2019
