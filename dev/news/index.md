# Changelog

## vroom (development version)

- [vroom.tidyverse.org](https://vroom.tidyverse.org/) is the new home of
  vroom’s website, catching up to the much earlier move (April 2022) of
  vroom’s GitHub repository from the r-lib organization to the
  tidyverse. The motivation for that was to make it easier to transfer
  issues between these two closely connected packages.

- The `path` parameter has been removed from
  [`vroom_write()`](https://vroom.tidyverse.org/dev/reference/vroom_write.md).
  This parameter was deprecated in vroom 1.5.0 (2021-06-14) in favor of
  the `file` parameter
  ([\#575](https://github.com/tidyverse/vroom/issues/575)).

- The function `vroom_altrep_opts()` and the argument
  `vroom(altrep_opts =)` have been removed. They were deprecated in
  favor of
  [`vroom_altrep()`](https://vroom.tidyverse.org/dev/reference/vroom_altrep.md)
  and `altrep =`, respectively, in v1.2.0 (2020-01-13). Also applies to
  `vroom_fwf(altrep_opts =)` and `vroom_lines(altrep_opts =)`
  ([\#575](https://github.com/tidyverse/vroom/issues/575)).

- Columns specified as having type “number” (requested via
  [`col_number()`](https://vroom.tidyverse.org/dev/reference/cols.md) or
  `"number"` or `'n'`) or “skip” (requested via
  [`col_skip()`](https://vroom.tidyverse.org/dev/reference/cols.md) or
  `"skip"` or `_` or `-`) now work in the case where 0 rows of data are
  parsed ([\#427](https://github.com/tidyverse/vroom/issues/427),
  [\#540](https://github.com/tidyverse/vroom/issues/540),
  [\#548](https://github.com/tidyverse/vroom/issues/548)).

- [`vroom()`](https://vroom.tidyverse.org/dev/reference/vroom.md),
  [`vroom_lines()`](https://vroom.tidyverse.org/dev/reference/vroom_lines.md),
  and
  [`vroom_fwf()`](https://vroom.tidyverse.org/dev/reference/vroom_fwf.md)
  now close and destroy (instead of leak) the connection in the case
  where opening the connection fails due to, e.g., a nonexistent URL
  ([\#488](https://github.com/tidyverse/vroom/issues/488)).

- vroom takes the recommended approach for phasing out usage of the
  non-API entry points `SETLENGTH` and `SET_TRUELENGTH`
  ([\#582](https://github.com/tidyverse/vroom/issues/582)).

- If there is insufficient space for the tempfile used when reading from
  a connection (affects delimited and fixed width parsing, from
  compressed files and URLs), that is now reported as an error and no
  longer segfaults
  ([\#544](https://github.com/tidyverse/vroom/issues/544)).

- `vroom(..., n_max = 0, col_names = c(...))` with a connection
  (compressed file, URL, raw connection) no longer produces a “negative
  length vectors are not allowed” error or crashes R
  ([\#539](https://github.com/tidyverse/vroom/issues/539)).

- `vroom_fwf(..., n_max = 0)` with a connection no longer segfaults
  ([\#590](https://github.com/tidyverse/vroom/issues/590)).

## vroom 1.6.7

CRAN release: 2025-11-28

- `locale(encoding =)` now warns, instead of errors, when the encoding
  cannot be found in [`iconvlist()`](https://rdrr.io/r/base/iconv.html)
  return value. This removes an unnecessary blocker on platforms like
  Alpine Linux where the output doesn’t reflect actual capabilities.

- vroom no longer uses `STDVEC_DATAPTR()` and takes the recommended
  approach for phasing out usage of `DATAPTR()`
  ([\#561](https://github.com/tidyverse/vroom/issues/561)).

- [`problems()`](https://vroom.tidyverse.org/dev/reference/problems.md)
  works normally for vroom-produced objects, even if readr is attached
  ([\#534](https://github.com/tidyverse/vroom/issues/534),
  [\#554](https://github.com/tidyverse/vroom/issues/554)).

- [`problems()`](https://vroom.tidyverse.org/dev/reference/problems.md)
  are no longer corrupted if the offending data frame is partially
  materialized, e.g. by viewing a subset, before calling
  [`problems()`](https://vroom.tidyverse.org/dev/reference/problems.md)
  ([\#535](https://github.com/tidyverse/vroom/issues/535)).

## vroom 1.6.6

CRAN release: 2025-09-19

- Fixed a bad URL in the README at CRAN’s request.

## vroom 1.6.5

CRAN release: 2023-12-05

- Internal changes requested by CRAN around format specification
  ([\#524](https://github.com/tidyverse/vroom/issues/524)).

## vroom 1.6.4

CRAN release: 2023-10-02

- It is now possible (again?) to read from a list of connections
  ([@bairdj](https://github.com/bairdj),
  [\#514](https://github.com/tidyverse/vroom/issues/514)).

- Internal change for compatibility with cpp11 \>= 0.4.6
  ([@DavisVaughan](https://github.com/DavisVaughan),
  [\#512](https://github.com/tidyverse/vroom/issues/512)).

## vroom 1.6.3

CRAN release: 2023-04-28

- No user-facing changes.

## vroom 1.6.2

- There was no CRAN release with this version number.

## vroom 1.6.1

CRAN release: 2023-01-22

- [`str()`](https://rdrr.io/r/utils/str.html) now works in a colorized
  context in the presence of a column of class `integer64`, i.e. parsed
  with
  [`col_big_integer()`](https://vroom.tidyverse.org/dev/reference/cols.md)
  ([@bart1](https://github.com/bart1),
  [\#477](https://github.com/tidyverse/vroom/issues/477)).

- The embedded implementation of the Grisu algorithm for printing
  floating point numbers now uses `snprintf()` instead of
  [`sprintf()`](https://rdrr.io/r/base/sprintf.html) and likewise for
  vroom’s own code ([@jeroen](https://github.com/jeroen),
  [\#480](https://github.com/tidyverse/vroom/issues/480)).

## vroom 1.6.0

CRAN release: 2022-09-30

- `vroom(col_select=)` now handles column selection by numeric position
  when `id` column is provided
  ([\#455](https://github.com/tidyverse/vroom/issues/455)).

- `vroom(id = "path", col_select = a:c)` is treated like
  `vroom(id = "path", col_select = c(path, a:c))`. If an `id` column is
  provided, it is automatically included in the output
  ([\#416](https://github.com/tidyverse/vroom/issues/416)).

- `vroom_write(append = TRUE)` does not modify an existing file when
  appending an empty data frame. In particular, it does not overwrite
  (delete) the existing contents of that file
  (<https://github.com/tidyverse/readr/issues/1408>,
  [\#451](https://github.com/tidyverse/vroom/issues/451)).

- [`vroom::problems()`](https://vroom.tidyverse.org/dev/reference/problems.md)
  now defaults to `.Last.value` for its primary input, similar to how
  `readr::problems()` works
  ([\#443](https://github.com/tidyverse/vroom/issues/443)).

- The warning that indicates the existence of parsing problems has been
  improved, which should make it easier for the user to follow-up
  (<https://github.com/tidyverse/readr/issues/1322>).

- [`vroom()`](https://vroom.tidyverse.org/dev/reference/vroom.md) reads
  more reliably from filepaths containing non-ascii characters, in a
  non-UTF-8 locale
  ([\#394](https://github.com/tidyverse/vroom/issues/394),
  [\#438](https://github.com/tidyverse/vroom/issues/438)).

- [`vroom_format()`](https://vroom.tidyverse.org/dev/reference/vroom_format.md)
  and
  [`vroom_write()`](https://vroom.tidyverse.org/dev/reference/vroom_write.md)
  only quote values that contain a delimiter, quote, or newline.
  Specifically values that are equal to the `na` string (or that start
  with it) are no longer quoted
  ([\#426](https://github.com/tidyverse/vroom/issues/426)).

- Fixed segfault when reading in multiple files and the first file has
  only a header row of column names, but subsequent files have at least
  one row ([\#430](https://github.com/tidyverse/vroom/issues/430)).

- Fixed segfault when
  [`vroom_format()`](https://vroom.tidyverse.org/dev/reference/vroom_format.md)
  is given an empty data frame
  ([\#425](https://github.com/tidyverse/vroom/issues/425))

- Fixed a segfault that could occur when the final field of the final
  line is missing and the file also does not end in a newline
  ([\#429](https://github.com/tidyverse/vroom/issues/429)).

- Fixed recursive garbage collection error that could occur during
  [`vroom_write()`](https://vroom.tidyverse.org/dev/reference/vroom_write.md)
  when
  [`output_column()`](https://vroom.tidyverse.org/dev/reference/output_column.md)
  generates an ALTREP vector
  ([\#389](https://github.com/tidyverse/vroom/issues/389)).

- [`vroom_progress()`](https://vroom.tidyverse.org/dev/reference/vroom_progress.md)
  uses
  [`rlang::is_interactive()`](https://rlang.r-lib.org/reference/is_interactive.html)
  instead of
  [`base::interactive()`](https://rdrr.io/r/base/interactive.html).

- `col_factor(levels = NULL)` honors the `na` strings of
  [`vroom()`](https://vroom.tidyverse.org/dev/reference/vroom.md) and
  its own `include_na` argument, as described in the docs, and now
  reproduces the behaviour of readr’s first edition parser
  ([\#396](https://github.com/tidyverse/vroom/issues/396)).

## vroom 1.5.7

CRAN release: 2021-11-30

- Jenny Bryan is now the official maintainer.

- Fix uninitialized bool detected by CRAN’s UBSAN check
  (<https://github.com/tidyverse/vroom/pull/386>)

- Fix buffer overflow when trying to parse an integer field that is over
  64 characters long (<https://github.com/tidyverse/readr/issues/1326>)

- Fix subset indexing when indexes span a file boundary multiple times
  ([\#383](https://github.com/tidyverse/vroom/issues/383))

## vroom 1.5.6

CRAN release: 2021-11-10

- `vroom(col_select=)` now works if `col_names = FALSE` as intended
  ([\#381](https://github.com/tidyverse/vroom/issues/381))

- `vroom(n_max=)` now correctly handles cases when reading from a
  connection and the file does *not* end with a newline
  (<https://github.com/tidyverse/readr/issues/1321>)

- [`vroom()`](https://vroom.tidyverse.org/dev/reference/vroom.md) no
  longer issues a spurious warning when the parsing needs to be
  restarted due to the presence of embedded newlines
  (<https://github.com/tidyverse/readr/issues/1313>)

- Fix performance issue when materializing subsetted vectors
  ([\#378](https://github.com/tidyverse/vroom/issues/378))

- [`vroom_format()`](https://vroom.tidyverse.org/dev/reference/vroom_format.md)
  now uses the same internal multi-threaded code as
  [`vroom_write()`](https://vroom.tidyverse.org/dev/reference/vroom_write.md),
  improving its performance in most cases
  ([\#377](https://github.com/tidyverse/vroom/issues/377))

- [`vroom_fwf()`](https://vroom.tidyverse.org/dev/reference/vroom_fwf.md)
  no longer omits the last line if it does *not* end with a newline
  (<https://github.com/tidyverse/readr/issues/1293>)

- Empty files or files with only a header line and no data no longer
  cause a crash if read with multiple files
  (<https://github.com/tidyverse/readr/issues/1297>)

- Files with a header but no contents, or a empty file if
  `col_names = FALSE` no longer cause a hang when `progress = TRUE`
  (<https://github.com/tidyverse/readr/issues/1297>)

- Commented lines with comments at the end of lines no longer hang R
  (<https://github.com/tidyverse/readr/issues/1309>)

- Comment lines containing unpaired quotes are no longer treated as
  unterminated quotations
  (<https://github.com/tidyverse/readr/issues/1307>)

- Values with only a `Inf` or `NaN` prefix but additional data
  afterwards, like `Inform` or no longer inappropriately guessed as
  doubles (<https://github.com/tidyverse/readr/issues/1319>)

- Time types now support `%h` format to denote hour durations greater
  than 24, like readr (<https://github.com/tidyverse/readr/issues/1312>)

- Fix performance issue when materializing subsetted vectors
  ([\#378](https://github.com/tidyverse/vroom/issues/378))

## vroom 1.5.5

CRAN release: 2021-09-14

- [`vroom()`](https://vroom.tidyverse.org/dev/reference/vroom.md) now
  supports files with only carriage return newlines (`\r`).
  ([\#360](https://github.com/tidyverse/vroom/issues/360),
  <https://github.com/tidyverse/readr/issues/1236>)

- [`vroom()`](https://vroom.tidyverse.org/dev/reference/vroom.md) now
  parses single digit datetimes more consistently as readr has done
  (<https://github.com/tidyverse/readr/issues/1276>)

- [`vroom()`](https://vroom.tidyverse.org/dev/reference/vroom.md) now
  parses `Inf` values as doubles
  (<https://github.com/tidyverse/readr/issues/1283>)

- [`vroom()`](https://vroom.tidyverse.org/dev/reference/vroom.md) now
  parses `NaN` values as doubles
  (<https://github.com/tidyverse/readr/issues/1277>)

- `VROOM_CONNECTION_SIZE` is now parsed as a double, which supports
  scientific notation
  ([\#364](https://github.com/tidyverse/vroom/issues/364))

- [`vroom()`](https://vroom.tidyverse.org/dev/reference/vroom.md) now
  works around specifying a `\n` as the delimiter
  ([\#365](https://github.com/tidyverse/vroom/issues/365),
  <https://github.com/tidyverse/dplyr/issues/5977>)

- [`vroom()`](https://vroom.tidyverse.org/dev/reference/vroom.md) no
  longer crashes if given a `col_name` and `col_type` both less than the
  number of columns (<https://github.com/tidyverse/readr/issues/1271>)

- [`vroom()`](https://vroom.tidyverse.org/dev/reference/vroom.md) no
  longer hangs if given an empty value for `locale(grouping_mark=)`
  (<https://github.com/tidyverse/readr/issues/1241>)

- Fix performance regression when guessing with large numbers of rows
  (<https://github.com/tidyverse/readr/issues/1267>)

## vroom 1.5.4

CRAN release: 2021-08-05

- `vroom(col_types=)` now accepts column type names like those accepted
  by utils::read.table. e.g. vroom::vroom(col_types = list(a =
  “integer”, b = “double”, c = “skip”))

- [`vroom()`](https://vroom.tidyverse.org/dev/reference/vroom.md) now
  respects the `quote` parameter properly in the first two lines of the
  file (<https://github.com/tidyverse/readr/issues/1262>)

- [`vroom_write()`](https://vroom.tidyverse.org/dev/reference/vroom_write.md)
  now always correctly writes its output including column names in UTF-8
  (<https://github.com/tidyverse/readr/issues/1242>)

- [`vroom_write()`](https://vroom.tidyverse.org/dev/reference/vroom_write.md)
  now creates an empty file when given a input without any columns
  (<https://github.com/tidyverse/readr/issues/1234>)

## vroom 1.5.3

CRAN release: 2021-07-14

- `vroom(col_types=)` now truncates the column types if the user passes
  too many types.
  ([\#355](https://github.com/tidyverse/vroom/issues/355))

- [`vroom()`](https://vroom.tidyverse.org/dev/reference/vroom.md) now
  always includes the last row when guessing
  ([\#352](https://github.com/tidyverse/vroom/issues/352))

- `vroom(trim_ws = TRUE)` now trims field content within quotes as well
  as without ([\#354](https://github.com/tidyverse/vroom/issues/354)).
  Previously vroom explicitly retained field content inside quotes
  regardless of the value of `trim_ws`.

## vroom 1.5.2

CRAN release: 2021-07-08

- [`vroom()`](https://vroom.tidyverse.org/dev/reference/vroom.md) now
  supports inputs with unnamed column types that are less than the
  number of columns
  ([\#296](https://github.com/tidyverse/vroom/issues/296))

- [`vroom()`](https://vroom.tidyverse.org/dev/reference/vroom.md) now
  outputs the correct column names even in the presence of skipped
  columns ([\#293](https://github.com/tidyverse/vroom/issues/293),
  [tidyverse/readr#1215](https://github.com/tidyverse/readr/issues/1215))

- `vroom_fwf(n_max=)` now works as intended when the input is a
  connection.

- [`vroom()`](https://vroom.tidyverse.org/dev/reference/vroom.md) and
  [`vroom_write()`](https://vroom.tidyverse.org/dev/reference/vroom_write.md)
  now automatically detect the compression format regardless of the file
  extension for bzip2, xzip, gzip and zip files
  ([\#348](https://github.com/tidyverse/vroom/issues/348))

- [`vroom()`](https://vroom.tidyverse.org/dev/reference/vroom.md) and
  [`vroom_write()`](https://vroom.tidyverse.org/dev/reference/vroom_write.md)
  now automatically support many more archive formats thanks to the
  archive package. These include new support for writing zip files,
  reading and writing 7zip, tar and ISO files.

- `vroom(num_threads = 1)` will now not spawn any threads. This can be
  used on as a workaround on systems without full thread support.

- Threads are now automatically disabled on non-macOS systems compiling
  against clang’s libc++. Most systems non-macOS systems use the more
  common gcc libstdc++, so this should not effect most users.

## vroom 1.5.1

CRAN release: 2021-06-22

- Parsers now treat NA values as NA even if they are valid values for
  the types ([\#342](https://github.com/tidyverse/vroom/issues/342))

- Element-wise indexing into lazy (ALTREP) vectors now has much less
  overhead ([\#344](https://github.com/tidyverse/vroom/issues/344))

## vroom 1.5.0

CRAN release: 2021-06-14

### Major improvements

- New `vroom(show_col_types=)` argument to more simply control when
  column types are shown.

- [`vroom()`](https://vroom.tidyverse.org/dev/reference/vroom.md),
  [`vroom_fwf()`](https://vroom.tidyverse.org/dev/reference/vroom_fwf.md)
  and
  [`vroom_lines()`](https://vroom.tidyverse.org/dev/reference/vroom_lines.md)
  now support multi-byte encodings such as UTF-16 and UTF-32 by
  converting these files to UTF-8 under the hood
  ([\#138](https://github.com/tidyverse/vroom/issues/138))

- [`vroom()`](https://vroom.tidyverse.org/dev/reference/vroom.md) now
  supports skipping comments and blank lines within data, not just at
  the start of the file
  ([\#294](https://github.com/tidyverse/vroom/issues/294),
  [\#302](https://github.com/tidyverse/vroom/issues/302))

- [`vroom()`](https://vroom.tidyverse.org/dev/reference/vroom.md) now
  uses the tzdb package when parsing date-times
  ([@DavisVaughan](https://github.com/DavisVaughan),
  [\#273](https://github.com/tidyverse/vroom/issues/273))

- [`vroom()`](https://vroom.tidyverse.org/dev/reference/vroom.md) now
  emits a warning of class `vroom_parse_issue` if there are non-fatal
  parsing issues.

- [`vroom()`](https://vroom.tidyverse.org/dev/reference/vroom.md) now
  emits a warning of class `vroom_mismatched_column_name` if the user
  supplies a column type that does not match the name of a read column
  ([\#317](https://github.com/tidyverse/vroom/issues/317)).

- The vroom package now uses the MIT license, as part of systematic
  relicensing throughout the r-lib and tidyverse packages
  ([\#323](https://github.com/tidyverse/vroom/issues/323))

### Minor improvements and fixes

- \`vroom() correctly reads double values with comma as decimal
  separator ([@kent37](https://github.com/kent37)
  [\#313](https://github.com/tidyverse/vroom/issues/313))

- [`vroom()`](https://vroom.tidyverse.org/dev/reference/vroom.md) now
  correctly skips lines with only one quote if the format doesn’t use
  quoting
  (<https://github.com/tidyverse/readr/issues/991#issuecomment-616378446>)

- [`vroom()`](https://vroom.tidyverse.org/dev/reference/vroom.md) and
  [`vroom_lines()`](https://vroom.tidyverse.org/dev/reference/vroom_lines.md)
  now handle files with mixed windows and POSIX line endings
  (<https://github.com/tidyverse/readr/issues/1210>)

- [`vroom()`](https://vroom.tidyverse.org/dev/reference/vroom.md) now
  outputs a tibble with the expected number of columns and types based
  on `col_types` and `col_names` even if the file is empty
  ([\#297](https://github.com/tidyverse/vroom/issues/297)).

- [`vroom()`](https://vroom.tidyverse.org/dev/reference/vroom.md) no
  longer mis-indexes files read from connections with windows line
  endings when the two line endings falls on separate sides of the read
  buffer ([\#331](https://github.com/tidyverse/vroom/issues/331))

- [`vroom()`](https://vroom.tidyverse.org/dev/reference/vroom.md) no
  longer crashes if `n_max = 0` and `col_names` is a character
  ([\#316](https://github.com/tidyverse/vroom/issues/316))

- [`vroom()`](https://vroom.tidyverse.org/dev/reference/vroom.md) now
  preserves the spec attribute when vroom and readr are both loaded
  ([\#303](https://github.com/tidyverse/vroom/issues/303))

- [`vroom()`](https://vroom.tidyverse.org/dev/reference/vroom.md) now
  allows specifying column names in `col_types` that have been repaired
  ([\#311](https://github.com/tidyverse/vroom/issues/311))

- [`vroom()`](https://vroom.tidyverse.org/dev/reference/vroom.md) no
  longer inadvertently calls `.name_repair` functions twice
  ([\#310](https://github.com/tidyverse/vroom/issues/310)).

- [`vroom()`](https://vroom.tidyverse.org/dev/reference/vroom.md) is now
  more robust to quoting issues when tracking the CSV state
  ([\#301](https://github.com/tidyverse/vroom/issues/301))

- [`vroom()`](https://vroom.tidyverse.org/dev/reference/vroom.md) now
  registers the S3 class with
  [`methods::setOldClass()`](https://rdrr.io/r/methods/setOldClass.html)
  (r-dbi/DBI#345)

- [`col_datetime()`](https://vroom.tidyverse.org/dev/reference/cols.md)
  now supports ‘%s’ format, which represents decimal seconds since the
  Unix epoch.

- `col_numeric()` now supports `grouping_mark` and `decimal_mark` that
  are unicode characters, such as U+00A0 which is commonly used as the
  grouping mark for numbers in France
  (<https://github.com/tidyverse/readr/issues/796>).

- [`vroom_fwf()`](https://vroom.tidyverse.org/dev/reference/vroom_fwf.md)
  gains a `skip_empty_rows` argument to skip empty lines
  (<https://github.com/tidyverse/readr/issues/1211>)

- [`vroom_fwf()`](https://vroom.tidyverse.org/dev/reference/vroom_fwf.md)
  now respects `n_max`, as intended
  ([\#334](https://github.com/tidyverse/vroom/issues/334))

- [`vroom_lines()`](https://vroom.tidyverse.org/dev/reference/vroom_lines.md)
  gains a `na` argument.

- [`vroom_write_lines()`](https://vroom.tidyverse.org/dev/reference/vroom_write_lines.md)
  no longer escapes or quotes lines.

- [`vroom_write_lines()`](https://vroom.tidyverse.org/dev/reference/vroom_write_lines.md)
  now works as intended
  ([\#291](https://github.com/tidyverse/vroom/issues/291)).

- `vroom_write(path=)` has been deprecated, in favor of `file`, to match
  readr.

- [`vroom_write_lines()`](https://vroom.tidyverse.org/dev/reference/vroom_write_lines.md)
  now exposes the `num_threads` argument.

- [`problems()`](https://vroom.tidyverse.org/dev/reference/problems.md)
  now prints the correct row number of parse errors
  ([\#326](https://github.com/tidyverse/vroom/issues/326))

- [`problems()`](https://vroom.tidyverse.org/dev/reference/problems.md)
  now throws a more informative error if called on a readr object
  ([\#308](https://github.com/tidyverse/vroom/issues/308)).

- [`problems()`](https://vroom.tidyverse.org/dev/reference/problems.md)
  now de-duplicates identical problems
  ([\#318](https://github.com/tidyverse/vroom/issues/318))

- Fix an inadvertent performance regression when reading values
  ([\#309](https://github.com/tidyverse/vroom/issues/309))

- `n_max` argument is correctly respected in edge cases
  ([\#306](https://github.com/tidyverse/vroom/issues/306))

- factors with implicit levels now work when fields are quoted, as
  intended ([\#330](https://github.com/tidyverse/vroom/issues/330))

- Guessing double types no longer unconditionally ignores leading
  whitespace. Now whitespace is only ignored when `trim_ws` is set.

## vroom 1.4.0

CRAN release: 2021-02-01

### Major changes and new functions

- vroom now tracks indexing and parsing errors like readr. The first
  time an issue is encountered a warning will be signaled. A tibble of
  all found problems can be retrieved with
  [`vroom::problems()`](https://vroom.tidyverse.org/dev/reference/problems.md).
  ([\#247](https://github.com/tidyverse/vroom/issues/247))

- Data with newlines within quoted fields will now automatically revert
  to using a single thread and be properly read
  ([\#282](https://github.com/tidyverse/vroom/issues/282))

- NUL values in character data are now permitted, with a warning.

- New
  [`vroom_write_lines()`](https://vroom.tidyverse.org/dev/reference/vroom_write_lines.md)
  function to write a character vector to a file
  ([\#291](https://github.com/tidyverse/vroom/issues/291))

- [`vroom_write()`](https://vroom.tidyverse.org/dev/reference/vroom_write.md)
  gains a `eol=` parameter to specify the end of line character(s) to
  use. Use `vroom_write(eol = "\r\n")` to write a file with Windows
  style newlines
  ([\#263](https://github.com/tidyverse/vroom/issues/263)).

### Minor improvements and fixes

- Datetime formats used when guessing now match those used when parsing
  ([\#240](https://github.com/tidyverse/vroom/issues/240))

- Quotes are now only valid next to newlines or delimiters
  ([\#224](https://github.com/tidyverse/vroom/issues/224))

- [`vroom()`](https://vroom.tidyverse.org/dev/reference/vroom.md) now
  signals an R error for invalid date and datetime formats, instead of
  crashing the session
  ([\#220](https://github.com/tidyverse/vroom/issues/220)).

- `vroom(comment = )` now accepts multi-character comments
  ([\#286](https://github.com/tidyverse/vroom/issues/286))

- [`vroom_lines()`](https://vroom.tidyverse.org/dev/reference/vroom_lines.md)
  now works with empty files
  ([\#285](https://github.com/tidyverse/vroom/issues/285))

- Vectors are now subset properly when given invalid subscripts
  ([\#283](https://github.com/tidyverse/vroom/issues/283))

- [`vroom_write()`](https://vroom.tidyverse.org/dev/reference/vroom_write.md)
  now works when the delimiter is empty, e.g. `delim = ""`
  ([\#287](https://github.com/tidyverse/vroom/issues/287)).

- [`vroom_write()`](https://vroom.tidyverse.org/dev/reference/vroom_write.md)
  now works with all ALTREP vectors, including string vectors
  ([\#270](https://github.com/tidyverse/vroom/issues/270))

- An internal call to
  [`new.env()`](https://rdrr.io/r/base/environment.html) now correctly
  uses the `parent` argument
  ([\#281](https://github.com/tidyverse/vroom/issues/281))

## vroom 1.3.2

CRAN release: 2020-09-30

- Test failures on R 4.1 related to factors with NA values fixed
  ([\#262](https://github.com/tidyverse/vroom/issues/262))

- [`vroom()`](https://vroom.tidyverse.org/dev/reference/vroom.md) now
  works without error with readr versions of col specs
  ([\#256](https://github.com/tidyverse/vroom/issues/256),
  [\#264](https://github.com/tidyverse/vroom/issues/264),
  [\#266](https://github.com/tidyverse/vroom/issues/266))

## vroom 1.3.1

CRAN release: 2020-08-27

- Test failures on R 4.1 related to POSIXct classes fixed
  ([\#260](https://github.com/tidyverse/vroom/issues/260))

- Column subsetting with double indexes now works again
  ([\#257](https://github.com/tidyverse/vroom/issues/257))

- `vroom(n_max=)` now only partially downloads files from connections,
  as intended ([\#259](https://github.com/tidyverse/vroom/issues/259))

## vroom 1.3.0

CRAN release: 2020-08-14

- The Rcpp dependency has been removed in favor of cpp11.

- [`vroom()`](https://vroom.tidyverse.org/dev/reference/vroom.md) now
  handles cases when `id` is set and a column in skipped
  ([\#237](https://github.com/tidyverse/vroom/issues/237))

- [`vroom()`](https://vroom.tidyverse.org/dev/reference/vroom.md) now
  supports column selections when there are some empty column names
  ([\#238](https://github.com/tidyverse/vroom/issues/238))

- [`vroom()`](https://vroom.tidyverse.org/dev/reference/vroom.md)
  argument `n_max` now works properly for files with windows newlines
  and no final newline
  ([\#244](https://github.com/tidyverse/vroom/issues/244))

- Subsetting vectors now works with
  [`View()`](https://rdrr.io/r/utils/View.html) in RStudio if there are
  now rows to subset
  ([\#253](https://github.com/tidyverse/vroom/issues/253)).

- Subsetting datetime columns now works with `NA` indices
  ([\#236](https://github.com/tidyverse/vroom/issues/236)).

## vroom 1.2.1

CRAN release: 2020-05-12

- [`vroom()`](https://vroom.tidyverse.org/dev/reference/vroom.md) now
  writes the column names if given an input with no rows
  ([\#213](https://github.com/tidyverse/vroom/issues/213))

- [`vroom()`](https://vroom.tidyverse.org/dev/reference/vroom.md)
  columns now support indexing with NA values
  ([\#201](https://github.com/tidyverse/vroom/issues/201))

- [`vroom()`](https://vroom.tidyverse.org/dev/reference/vroom.md) no
  longer truncates the last value in a file if the file contains windows
  newlines but no final newline
  ([\#219](https://github.com/tidyverse/vroom/issues/219)).

- [`vroom()`](https://vroom.tidyverse.org/dev/reference/vroom.md) now
  works when the `na` argument is encoded in non ASCII or UTF-8 locales
  *and* the file encoding is not the same as the native encoding
  ([\#233](https://github.com/tidyverse/vroom/issues/233)).

- [`vroom_fwf()`](https://vroom.tidyverse.org/dev/reference/vroom_fwf.md)
  now verifies that the positions are valid, namely that the begin value
  is always less than the previous end
  ([\#217](https://github.com/tidyverse/vroom/issues/217)).

- [`vroom_lines()`](https://vroom.tidyverse.org/dev/reference/vroom_lines.md)
  gains a `locale` argument so you can control the encoding of the file
  ([\#218](https://github.com/tidyverse/vroom/issues/218))

- [`vroom_write()`](https://vroom.tidyverse.org/dev/reference/vroom_write.md)
  now supports the `append` argument with R connections
  ([\#232](https://github.com/tidyverse/vroom/issues/232))

## vroom 1.2.0

CRAN release: 2020-01-13

### Breaking changes

- `vroom_altrep_opts()` and the argument `vroom(altrep_opts =)` have
  been renamed to
  [`vroom_altrep()`](https://vroom.tidyverse.org/dev/reference/vroom_altrep.md)
  and `altrep` respectively. The prior names have been deprecated.

### New Features

- [`vroom()`](https://vroom.tidyverse.org/dev/reference/vroom.md) now
  supports reading Big Integer values with the `bit64` package. Use
  [`col_big_integer()`](https://vroom.tidyverse.org/dev/reference/cols.md)
  or the “I” shortcut to read a column as big integers.
  ([\#198](https://github.com/tidyverse/vroom/issues/198))

- [`cols()`](https://vroom.tidyverse.org/dev/reference/cols.md) gains a
  `.delim` argument and
  [`vroom()`](https://vroom.tidyverse.org/dev/reference/vroom.md) now
  uses it as the delimiter if it is provided
  ([\#192](https://github.com/tidyverse/vroom/issues/192))

- [`vroom()`](https://vroom.tidyverse.org/dev/reference/vroom.md) now
  supports reading from
  [`stdin()`](https://rdrr.io/r/base/showConnections.html) directly,
  interpreted as the C-level standard input
  ([\#106](https://github.com/tidyverse/vroom/issues/106)).

### Minor improvements and fixes

- `col_date` now parses single digit month and day
  ([@edzer](https://github.com/edzer),
  [\#123](https://github.com/tidyverse/vroom/issues/123),
  [\#170](https://github.com/tidyverse/vroom/issues/170))

- [`fwf_empty()`](https://vroom.tidyverse.org/dev/reference/vroom_fwf.md)
  now uses the `skip` parameter, as intended.

- [`vroom()`](https://vroom.tidyverse.org/dev/reference/vroom.md) can
  now read single line files without a terminal newline
  ([\#173](https://github.com/tidyverse/vroom/issues/173)).

- [`vroom()`](https://vroom.tidyverse.org/dev/reference/vroom.md) can
  now select the id column if provided
  ([\#110](https://github.com/tidyverse/vroom/issues/110)).

- [`vroom()`](https://vroom.tidyverse.org/dev/reference/vroom.md) now
  correctly copies string data for factor levels
  ([\#184](https://github.com/tidyverse/vroom/issues/184))

- [`vroom()`](https://vroom.tidyverse.org/dev/reference/vroom.md) no
  longer crashes when files have trailing fields, windows newlines and
  the file is not newline or null terminated.

- [`vroom()`](https://vroom.tidyverse.org/dev/reference/vroom.md) now
  includes a spec object with the `col_types` class, as intended.

- [`vroom()`](https://vroom.tidyverse.org/dev/reference/vroom.md) now
  better handles floating point values with very large exponents
  ([\#164](https://github.com/tidyverse/vroom/issues/164)).

- [`vroom()`](https://vroom.tidyverse.org/dev/reference/vroom.md) now
  uses better heuristics to guess the delimiter and now throws an error
  if a delimiter cannot be guessed
  ([\#126](https://github.com/tidyverse/vroom/issues/126),
  [\#141](https://github.com/tidyverse/vroom/issues/141),
  [\#167](https://github.com/tidyverse/vroom/issues/167)).

- [`vroom()`](https://vroom.tidyverse.org/dev/reference/vroom.md) now
  has an improved error message when a file does not exist
  ([\#169](https://github.com/tidyverse/vroom/issues/169)).

- [`vroom()`](https://vroom.tidyverse.org/dev/reference/vroom.md) no
  longer leaks file handles
  ([\#177](https://github.com/tidyverse/vroom/issues/177),
  [\#180](https://github.com/tidyverse/vroom/issues/180))

- [`vroom()`](https://vroom.tidyverse.org/dev/reference/vroom.md) now
  outputs its messages on
  [`stdout()`](https://rdrr.io/r/base/showConnections.html) rather than
  [`stderr()`](https://rdrr.io/r/base/showConnections.html), which
  avoids the text being red in RStudio and in the Windows GUI.

- [`vroom()`](https://vroom.tidyverse.org/dev/reference/vroom.md) no
  longer overflows when reading files with more than 2B entries
  ([@wlattner](https://github.com/wlattner),
  [\#183](https://github.com/tidyverse/vroom/issues/183)).

- [`vroom_fwf()`](https://vroom.tidyverse.org/dev/reference/vroom_fwf.md)
  is now more robust if not all lines are the expected length
  ([\#78](https://github.com/tidyverse/vroom/issues/78))

- [`vroom_fwf()`](https://vroom.tidyverse.org/dev/reference/vroom_fwf.md)
  and
  [`fwf_empty()`](https://vroom.tidyverse.org/dev/reference/vroom_fwf.md)
  now support passing `Inf` to `guess_max()`.

- [`vroom_str()`](https://vroom.tidyverse.org/dev/reference/vroom_str.md)
  now works with S4 objects.

- [`vroom_fwf()`](https://vroom.tidyverse.org/dev/reference/vroom_fwf.md)
  now handles files with dos newlines properly.

- [`vroom_write()`](https://vroom.tidyverse.org/dev/reference/vroom_write.md)
  now does not try to write anything when given empty inputs
  ([\#172](https://github.com/tidyverse/vroom/issues/172)).

- Dates, times, and datetimes now properly consider the locale when
  parsing.

- Added benchmarks with *wide* data for both numeric and character data
  ([\#87](https://github.com/tidyverse/vroom/issues/87),
  [@R3myG](https://github.com/R3myG))

- The delimiter used for parsing is now shown in the message output
  ([\#95](https://github.com/tidyverse/vroom/issues/95)
  [@R3myG](https://github.com/R3myG))

## vroom 1.0.2

CRAN release: 2019-06-28

### New Features

- The column created by `id` is now stored as an run length encoded
  Altrep vector, which uses less memory and is much faster for large
  inputs. ([\#111](https://github.com/tidyverse/vroom/issues/111))

### Minor improvements and fixes

- [`vroom_lines()`](https://vroom.tidyverse.org/dev/reference/vroom_lines.md)
  now properly respects the `n_max` parameter
  ([\#142](https://github.com/tidyverse/vroom/issues/142))

- [`vroom()`](https://vroom.tidyverse.org/dev/reference/vroom.md) and
  [`vroom_lines()`](https://vroom.tidyverse.org/dev/reference/vroom_lines.md)
  now support reading files which do not end in newlines by using a file
  connection ([\#40](https://github.com/tidyverse/vroom/issues/40)).

- [`vroom_write()`](https://vroom.tidyverse.org/dev/reference/vroom_write.md)
  now works with the standard output connection
  [`stdout()`](https://rdrr.io/r/base/showConnections.html)
  ([\#106](https://github.com/tidyverse/vroom/issues/106)).

- [`vroom_write()`](https://vroom.tidyverse.org/dev/reference/vroom_write.md)
  no longer crashes non-deterministically when used on Altrep vectors.

- The integer parser now returns NA values for invalid inputs
  ([\#135](https://github.com/tidyverse/vroom/issues/135))

- Fix additional UBSAN issue in the mio project reported by CRAN
  ([\#97](https://github.com/tidyverse/vroom/issues/97))

- Fix indexing into connections with quoted fields
  ([\#119](https://github.com/tidyverse/vroom/issues/119))

- Move example files for
  [`vroom()`](https://vroom.tidyverse.org/dev/reference/vroom.md) out of
  `\dontshow{}`.

- Fix integer overflow with very large files
  ([\#116](https://github.com/tidyverse/vroom/issues/116),
  [\#119](https://github.com/tidyverse/vroom/issues/119))

- Fix missing columns and windows newlines
  ([\#114](https://github.com/tidyverse/vroom/issues/114))

- Fix encoding of column names
  ([\#113](https://github.com/tidyverse/vroom/issues/113),
  [\#115](https://github.com/tidyverse/vroom/issues/115))

- Throw an error message when writing a zip file, which is not supported
  ([@metaOO](https://github.com/metaOO),
  [\#145](https://github.com/tidyverse/vroom/issues/145))

- Default message output from
  [`vroom()`](https://vroom.tidyverse.org/dev/reference/vroom.md) now
  uses `Rows` and `Cols` ([@meta00](https://github.com/meta00),
  [\#140](https://github.com/tidyverse/vroom/issues/140))

## vroom 1.0.1

CRAN release: 2019-05-14

### New Features

- [`vroom_lines()`](https://vroom.tidyverse.org/dev/reference/vroom_lines.md)
  function added, to (lazily) read lines from a file into a character
  vector ([\#90](https://github.com/tidyverse/vroom/issues/90)).

### Minor improvements and fixes

- Fix for a hang on Windows caused by a race condition in the progress
  bar ([\#98](https://github.com/tidyverse/vroom/issues/98))

- Remove accidental runtime dependency on testthat
  ([\#104](https://github.com/tidyverse/vroom/issues/104))

- Fix to actually return non-Altrep character columns on R 3.2, 3.3 and
  3.4.

- Disable colors in the progress bar when running in RStudio, to work
  around an issue where the progress bar would be garbled
  (<https://github.com/rstudio/rstudio/issues/4777>)

- Fix for UBSAN issues reported by CRAN
  ([\#97](https://github.com/tidyverse/vroom/issues/97))

- Fix for rchk issues reported by CRAN
  ([\#94](https://github.com/tidyverse/vroom/issues/94))

- The progress bar now only updates every 10 milliseconds.

- Getting started vignette index entry now more informative
  ([\#92](https://github.com/tidyverse/vroom/issues/92))

## vroom 1.0.0

CRAN release: 2019-05-04

- Initial release

- Added a `NEWS.md` file to track changes to the package.
