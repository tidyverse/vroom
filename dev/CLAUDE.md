# vroom: Read and Write Rectangular Text Data Quickly

This file provides guidance to Claude Code (claude.ai/code) when working
with code in this repository.

## Package Overview

vroom reads and writes rectangular text data (CSV, TSV, fixed-width
files). It uses R’s Altrep framework for lazy evaluation - indexing file
structure quickly, then parsing values on-demand as they’re accessed.
Multi-threading is used for indexing, materializing non-character
columns, and writing. vroom powers readr’s Edition 2 and is part of the
tidyverse ecosystem.

## Development Workflow

General advice: \* When running R from the console, prefer `Rscript`. \*
Always run `air format .` after generating or modifying R code. The
binary of air is probably not on the PATH but is typically found inside
the Air extension used by Positron, e.g. something like
`~/.positron/extensions/posit.air-vscode-0.18.0/bundled/bin/air`.

### Testing

- Tests for `R/{name}.R` go in `tests/testthat/test-{name}.R`.

- Use `devtools::test()` to run all tests

- Use `devtools::test(filter = "name")` to run tests for `R/{name}.R`

- DO NOT USE `devtools::test_active_file()`

- All testing functions automatically load code; you don’t need to.

- All new code should have an accompanying test.

- If there are existing tests, place new tests next to similar existing
  tests.

### Code style

- Use newspaper style/high-level first function organisation. Main logic
  at the top and helper functions should come below.
- Don’t define functions inside of functions unless they are very brief.
- Error messages should use
  [`cli::cli_abort()`](https://cli.r-lib.org/reference/cli_abort.html)
  and follow the tidyverse style guide
  (<https://style.tidyverse.org/errors.html>)

## Key Technical Details

**Architecture** - Two-phase operation: (1) quick multi-threaded
indexing to locate field boundaries and line positions, (2) lazy parsing
via Altrep vectors that parse values on access - C++ code uses cpp11
interface (in `src/`) with memory mapping (mio library) for efficient
file access - Main R code in `R/` provides user-facing API and column
specification system (shared with readr) - Key C++ components:
`delimited_index.cc/.h` (indexing), `altrep.cc/.h` (lazy vectors),
`collectors.h` (type-specific parsing), `DateTimeParser.h` (temporal
data)

**Core Functions** - Reading:
[`vroom()`](https://vroom.tidyverse.org/dev/reference/vroom.md) (main
delimited reader with delimiter guessing),
[`vroom_fwf()`](https://vroom.tidyverse.org/dev/reference/vroom_fwf.md)
(fixed-width files),
[`vroom_lines()`](https://vroom.tidyverse.org/dev/reference/vroom_lines.md)
(lazy line reading) - Writing:
[`vroom_write()`](https://vroom.tidyverse.org/dev/reference/vroom_write.md)
(async formatting, multi-threaded writing),
[`vroom_write_lines()`](https://vroom.tidyverse.org/dev/reference/vroom_write_lines.md),
[`vroom_format()`](https://vroom.tidyverse.org/dev/reference/vroom_format.md)
(format without writing to disk) - Fixed-width positioning:
[`fwf_empty()`](https://vroom.tidyverse.org/dev/reference/vroom_fwf.md)
(auto-detect),
[`fwf_widths()`](https://vroom.tidyverse.org/dev/reference/vroom_fwf.md)
(specify widths),
[`fwf_positions()`](https://vroom.tidyverse.org/dev/reference/vroom_fwf.md)
(start/end),
[`fwf_cols()`](https://vroom.tidyverse.org/dev/reference/vroom_fwf.md)
(named arguments) - Utilities:
[`problems()`](https://vroom.tidyverse.org/dev/reference/problems.md)
(inspect parsing issues),
[`spec()`](https://vroom.tidyverse.org/dev/reference/spec.md) (extract
column spec),
[`vroom_altrep()`](https://vroom.tidyverse.org/dev/reference/vroom_altrep.md)
(control Altrep usage),
[`vroom_str()`](https://vroom.tidyverse.org/dev/reference/vroom_str.md)
(structure display for Altrep objects)

**Column Type System** - Automatic type guessing from sample of rows
(controlled by `guess_max`) - Explicit specification via
[`cols()`](https://vroom.tidyverse.org/dev/reference/cols.md),
[`cols_only()`](https://vroom.tidyverse.org/dev/reference/cols.md), or
compact string notation - Column parsers:
[`col_character()`](https://vroom.tidyverse.org/dev/reference/cols.md),
[`col_integer()`](https://vroom.tidyverse.org/dev/reference/cols.md),
[`col_big_integer()`](https://vroom.tidyverse.org/dev/reference/cols.md),
[`col_double()`](https://vroom.tidyverse.org/dev/reference/cols.md),
[`col_number()`](https://vroom.tidyverse.org/dev/reference/cols.md),
[`col_date()`](https://vroom.tidyverse.org/dev/reference/cols.md),
[`col_datetime()`](https://vroom.tidyverse.org/dev/reference/cols.md),
[`col_time()`](https://vroom.tidyverse.org/dev/reference/cols.md),
[`col_factor()`](https://vroom.tidyverse.org/dev/reference/cols.md),
[`col_logical()`](https://vroom.tidyverse.org/dev/reference/cols.md),
[`col_skip()`](https://vroom.tidyverse.org/dev/reference/cols.md),
[`col_guess()`](https://vroom.tidyverse.org/dev/reference/cols.md) -
Compact string notation: `c`=character, `i`=integer, `I`=big integer,
`d`=double, `n`=number, `l`=logical, `f`=factor, `D`=date, `T`=datetime,
`t`=time, `?`=guess, `_` or `-`=skip - tidyselect-style column
selection:
[`starts_with()`](https://tidyselect.r-lib.org/reference/starts_with.html),
[`ends_with()`](https://tidyselect.r-lib.org/reference/starts_with.html),
[`contains()`](https://tidyselect.r-lib.org/reference/starts_with.html),
[`matches()`](https://tidyselect.r-lib.org/reference/starts_with.html),
etc. - Problems tracking:
[`problems()`](https://vroom.tidyverse.org/dev/reference/problems.md)
function to inspect parsing issues

**Locale Support** -
[`locale()`](https://vroom.tidyverse.org/dev/reference/locale.md) object
controls region-specific settings: decimal mark, grouping mark,
date/time formats, encoding, timezone - Defaults to US-centric locale
but fully customizable via
[`date_names()`](https://vroom.tidyverse.org/dev/reference/date_names.md)
and
[`date_names_langs()`](https://vroom.tidyverse.org/dev/reference/date_names.md)

**Performance & Parsing** - Multi-threaded indexing, materialization,
and writing (controlled by `num_threads` parameter or `VROOM_THREADS`
environment variable) - Altrep lazy evaluation enabled by default for
character vectors (controlled by `VROOM_USE_ALTREP_*` environment
variables) - Progress bars for long operations (controlled by
`VROOM_SHOW_PROGRESS` environment variable) - Memory mapping via mio
library for efficient file access - Support for reading from multiple
files, connections, URLs, compressed files - Delimiter guessing,
multi-byte delimiters, Unicode delimiters - Embedded newlines in fields
(requires `num_threads = 1`)

**Key Dependencies** - cli: Error messages and formatting - tibble:
Output format - tzdb: Timezone database for datetime parsing - vctrs:
Vector utilities - tidyselect: Column selection helpers - bit64: 64-bit
integer support - cpp11: R/C++ interface (LinkingTo) - withr: Temporary
options/environment variable management - hms, crayon, glue, lifecycle,
rlang: Supporting utilities

**Environment Variables** - `VROOM_THREADS`: Number of threads (default:
[`parallel::detectCores()`](https://rdrr.io/r/parallel/detectCores.html)) -
`VROOM_SHOW_PROGRESS`: Show progress bars (default: TRUE in interactive
sessions) - `VROOM_TEMP_PATH`: Directory for temporary connection
buffers (default: [`tempdir()`](https://rdrr.io/r/base/tempfile.html)) -
`VROOM_CONNECTION_SIZE`: Buffer size for connections (default: 128
KiB) - `VROOM_WRITE_BUFFER_LINES`: Lines per buffer when writing
(default: nrow/100/num_threads) - `VROOM_USE_ALTREP_CHR`: Enable Altrep
for characters (default: TRUE) - `VROOM_USE_ALTREP_*`: Control Altrep
for other types (FCT, INT, BIG_INT, DBL, NUM, LGL, DTTM, DATE, TIME,
NUMERICS)
