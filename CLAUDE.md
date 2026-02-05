# vroom: Read and Write Rectangular Text Data Quickly

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

We are working on a fork of the upstream project. PRs should be opened against jimhester/vroom, _not_ tidyverse/vroom.

## Package Overview

vroom reads and writes rectangular text data (CSV, TSV, fixed-width files). It uses R's Altrep framework for lazy evaluation - indexing file structure quickly, then parsing values on-demand as they're accessed. Multi-threading is used for indexing, materializing non-character columns, and writing. vroom powers readr's Edition 2 and is part of the tidyverse ecosystem.

## Development Workflow

General advice:
* When running R from the console, prefer `Rscript`.
* Always run `air format .` after generating or modifying R code. The binary of air is on the path.

### Testing

- Tests for `R/{name}.R` go in `tests/testthat/test-{name}.R`.
- Use `devtools::test()` to run all tests
- Use `devtools::test(filter = "name")` to run tests for `R/{name}.R`
- DO NOT USE `devtools::test_active_file()`
- All testing functions automatically load code; you don't need to.

- All new code should have an accompanying test.
- If there are existing tests, place new tests next to similar existing tests.

### Code style

- Use newspaper style/high-level first function organisation. Main logic at the top and helper functions should come below.
- Don't define functions inside of functions unless they are very brief.
- Error messages should use `cli::cli_abort()` and follow the tidyverse style guide (https://style.tidyverse.org/errors.html)

## libvroom Architecture

libvroom (`src/libvroom/`, vendored from `~/p/libvroom`) is a high-performance CSV parser using portable SIMD instructions (via Google Highway), based on a speculative multi-threaded two-pass algorithm from Chang et al. (SIGMOD 2019) and SIMD techniques from Langdale & Lemire (simdjson). It outputs parsed data in Arrow columnar format for zero-copy interop with R.

### Parsing Pipeline

Four-phase pipeline (Polars-inspired):

1. **SIMD Analysis** — Memory-map the file, detect encoding (UTF-8/16/32/Latin1 via BOM + heuristics), detect CSV dialect (delimiter, quote char) via consistency scoring. Dual-state chunk analysis: single SIMD pass computes stats for both starting quote states, then resolves via forward propagation. Optionally caches chunk boundaries to disk (`~/.cache/libvroom/`).
2. **Parallel Chunk Parsing** — Thread pool dispatches chunks to workers. Each thread uses `SplitFields` iterator (SIMD boundary caching, 64 bytes/iteration) and SIMD integer parsing (`simd_atoi`), appending directly to thread-local `ArrowColumnBuilder` instances.
3. **Type Inference** — Sample first N rows; try parsing as BOOL → INT32 → INT64 → FLOAT64 → DATE → TIMESTAMP → STRING, promoting types as needed.
4. **Column Building** — Workers build columns in Arrow format (packed null bitmap with lazy init, contiguous NumericBuffer/StringBuffer). Merge is O(1) for strings (move pointers), O(n) for numerics (copy+append).

### Directory Layout

```
src/libvroom/
├── include/libvroom/     # Public API headers
│   ├── libvroom.h        # Umbrella header (version 2.0.0)
│   ├── vroom.h           # CsvReader, MmapSource, ChunkFinder, TypeInference
│   ├── types.h           # DataType enum, FieldView, ColumnSchema, Result<T>
│   ├── options.h         # CsvOptions, ParquetOptions, ThreadOptions
│   ├── arrow_column_builder.h  # Arrow-format column builders (int32/64, float64, bool, string, date, timestamp)
│   ├── arrow_buffer.h    # NullBitmap, StringBuffer, NumericBuffer<T>
│   ├── table.h           # Multi-chunk Arrow table with ArrowArrayStream export
│   ├── error.h           # ErrorCode (17 types), ErrorMode (DISABLED/FAIL_FAST/PERMISSIVE/BEST_EFFORT)
│   ├── dialect.h         # CSV dialect detection
│   ├── streaming.h       # Streaming parser for large files
│   └── ...               # ~28 headers total
├── src/
│   ├── parser/           # SIMD field splitting, quote parity (CLMUL), SIMD integer parsing, chunk finding
│   ├── reader/           # CsvReader orchestration, memory-mapped source
│   ├── schema/           # Type inference, type parsers (fast_float, Highway SIMD, ISO8601)
│   ├── columns/          # Legacy ColumnBuilder (being replaced by ArrowColumnBuilder)
│   ├── writer/           # Parquet writer (multi-threaded encoding, Thrift metadata), Arrow IPC writer
│   ├── encoding/         # Parquet encodings: PLAIN, RLE, DELTA_BINARY_PACKED, DELTA_LENGTH_BYTE_ARRAY
│   ├── cache/            # Persistent index cache (Elias-Fano compressed, atomic writes)
│   └── simd/             # SIMD-accelerated statistics
└── third_party/
    ├── hwy/              # Google Highway — portable SIMD (x86 SSE4.2/AVX2, ARM NEON, scalar fallback)
    ├── simdutf/          # SIMD UTF-8/16/32 validation and transcoding
    ├── fast_float/       # Fast double parsing (~3x strtod)
    └── BS_thread_pool.hpp  # Single-header thread pool
```

### R Integration (src/)

The R package bridges libvroom's Arrow output to R data structures:

| File | Purpose |
|------|---------|
| `vroom_new.cpp` | New libvroom-based `vroom()` entry point: streaming API (`start_streaming()` / `next_chunk()`) |
| `arrow_to_r.cpp/.h` | Converts `ArrowColumnBuilder`s to R data frame; numeric cols copy to R vectors, string cols wrap in Altrep or materialize |
| `altrep.cc/.h` | R Altrep (lazy) vectors backed by Arrow string buffers — near-instant for deferred materialization |
| `vroom_arrow.cpp` | Arrow C Data Interface export (RecordBatch/Stream) |
| `cpp11.cpp` | Generated cpp11 bindings registering C++ functions callable from R |
| `delimited_index.cc/.h` | Legacy two-pass indexer (being replaced by libvroom) |
| `vroom_*.cc/.h` | Legacy per-type column implementations (being replaced) |

Integration flow:
```
R: vroom(path)
  → cpp11: vroom_libvroom_()
    → libvroom: CsvReader::open() + start_streaming()
      → Parallel SIMD parsing → ParsedChunks
    → arrow_to_r: columns_to_r() → R vectors
    → altrep: Wrap strings in Altrep (deferred materialization)
  → R: tibble returned to user
```

### Build (Makevars)

Source categories compiled into `vroom.so`:
- **VROOM_SOURCES** (13 files): Legacy vroom C++ implementation
- **LIBVROOM_SOURCES** (30 files): All libvroom implementation
- **SIMDUTF_SOURCES** (1 file): UTF transcoding
- **HIGHWAY_SOURCES** (3 files): Google Highway SIMD
- **Arrow integration** (5 files): arrow_to_r, vroom_arrow, vroom_new, etc.

Include paths: `-Imio/include`, `-Ispdlog/include`, `-Ilibvroom`, `-Ilibvroom/include`, `-Ilibvroom/third_party`

## R Package API

**Core Functions**
- Reading: `vroom()` (main delimited reader with delimiter guessing), `vroom_fwf()` (fixed-width files), `vroom_lines()` (lazy line reading)
- Writing: `vroom_write()` (async formatting, multi-threaded writing), `vroom_write_lines()`, `vroom_format()` (format without writing to disk)
- Fixed-width positioning: `fwf_empty()` (auto-detect), `fwf_widths()` (specify widths), `fwf_positions()` (start/end), `fwf_cols()` (named arguments)
- Utilities: `problems()` (inspect parsing issues), `spec()` (extract column spec), `vroom_altrep()` (control Altrep usage), `vroom_str()` (structure display for Altrep objects)

**Column Type System**
- Automatic type guessing from sample of rows (controlled by `guess_max`)
- Explicit specification via `cols()`, `cols_only()`, or compact string notation
- Column parsers: `col_character()`, `col_integer()`, `col_big_integer()`, `col_double()`, `col_number()`, `col_date()`, `col_datetime()`, `col_time()`, `col_factor()`, `col_logical()`, `col_skip()`, `col_guess()`
- Compact string notation: `c`=character, `i`=integer, `I`=big integer, `d`=double, `n`=number, `l`=logical, `f`=factor, `D`=date, `T`=datetime, `t`=time, `?`=guess, `_` or `-`=skip
- tidyselect-style column selection: `starts_with()`, `ends_with()`, `contains()`, `matches()`, etc.
- Problems tracking: `problems()` function to inspect parsing issues

**Locale Support**
- `locale()` object controls region-specific settings: decimal mark, grouping mark, date/time formats, encoding, timezone
- Defaults to US-centric locale but fully customizable via `date_names()` and `date_names_langs()`

**Key R Dependencies**
- cli: Error messages and formatting
- tibble: Output format
- tzdb: Timezone database for datetime parsing
- vctrs: Vector utilities
- tidyselect: Column selection helpers
- bit64: 64-bit integer support
- cpp11: R/C++ interface (LinkingTo)
- withr: Temporary options/environment variable management
- hms, crayon, glue, lifecycle, rlang: Supporting utilities

**Environment Variables**
- `VROOM_THREADS`: Number of threads (default: `parallel::detectCores()`)
- `VROOM_SHOW_PROGRESS`: Show progress bars (default: TRUE in interactive sessions)
- `VROOM_TEMP_PATH`: Directory for temporary connection buffers (default: `tempdir()`)
- `VROOM_CONNECTION_SIZE`: Buffer size for connections (default: 128 KiB)
- `VROOM_WRITE_BUFFER_LINES`: Lines per buffer when writing (default: nrow/100/num_threads)
- `VROOM_USE_ALTREP_CHR`: Enable Altrep for characters (default: TRUE)
- `VROOM_USE_ALTREP_*`: Control Altrep for other types (FCT, INT, BIG_INT, DBL, NUM, LGL, DTTM, DATE, TIME, NUMERICS)
