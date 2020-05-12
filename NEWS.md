# vroom 1.2.1

* `vroom()` now writes the column names if given an input with no rows (#213)

* `vroom()` columns now support indexing with NA values (#201)

* `vroom()` no longer truncates the last value in a file if the file contains windows newlines but no final newline (#219).

* `vroom()` now works when the `na` argument is encoded in non ASCII or UTF-8 locales _and_ the file encoding is not the same as the native encoding (#233).

* `vroom_fwf()` now verifies that the positions are valid, namely that the begin value is always less than the previous end (#217).

* `vroom_lines()` gains a `locale` argument so you can control the encoding of the file (#218)

* `vroom_write()` now supports the `append` argument with R connections (#232)

# vroom 1.2.0

## Breaking changes

* `vroom_altrep_opts()` and the argument `vroom(altrep_opts =)` have been
  renamed to `vroom_altrep()` and `altrep` respectively. The prior names have
  been deprecated.

## New Features

* `vroom()` now supports reading Big Integer values with the `bit64` package.
  Use `col_big_integer()` or the "I" shortcut to read a column as big integers. (#198)

* `cols()` gains a `.delim` argument and `vroom()` now uses it as the delimiter
  if it is provided (#192)

* `vroom()` now supports reading from `stdin()` directly, interpreted as the
  C-level standard input (#106).

## Minor improvements and fixes

* `col_date` now parses single digit month and day (@edzer, #123, #170)

* `fwf_empty()` now uses the `skip` parameter, as intended.

* `vroom()` can now read single line files without a terminal newline (#173).

* `vroom()` can now select the id column if provided (#110).

* `vroom()` now correctly copies string data for factor levels (#184)

* `vroom()` no longer crashes when files have trailing fields, windows newlines
  and the file is not newline or null terminated.

* `vroom()` now includes a spec object with the `col_types` class, as intended.

* `vroom()` now better handles floating point values with very large exponents
  (#164).

* `vroom()` now uses better heuristics to guess the delimiter and now throws an
  error if a delimiter cannot be guessed (#126, #141, #167).

* `vroom()` now has an improved error message when a file does not exist (#169).

* `vroom()` no longer leaks file handles (#177, #180)

* `vroom()` now outputs its messages on `stdout()` rather than `stderr()`,
  which avoids the text being red in RStudio and in the Windows GUI.

* `vroom()` no longer overflows when reading files with more than 2B entries (@wlattner, #183).

* `vroom_fwf()` is now more robust if not all lines are the expected length (#78)

* `vroom_fwf()` and `fwf_empty()` now support passing `Inf` to `guess_max()`.

* `vroom_str()` now works with S4 objects.

* `vroom_fwf()` now handles files with dos newlines properly.

* `vroom_write()` now does not try to write anything when given empty inputs (#172).

* Dates, times, and datetimes now properly consider the locale when parsing.

* Added benchmarks with _wide_ data for both numeric and character data (#87, @R3myG)

* The delimiter used for parsing is now shown in the message output (#95 @R3myG)

# vroom 1.0.2

## New Features

* The column created by `id` is now stored as an run length encoded Altrep
  vector, which uses less memory and is much faster for large inputs. (#111)

## Minor improvements and fixes

* `vroom_lines()` now properly respects the `n_max` parameter (#142)

* `vroom()` and `vroom_lines()` now support reading files which do not end in
  newlines by using a file connection (#40).

* `vroom_write()` now works with the standard output connection `stdout()` (#106).

* `vroom_write()` no longer crashes non-deterministically when used on Altrep vectors.

* The integer parser now returns NA values for invalid inputs (#135)

* Fix additional UBSAN issue in the mio project reported by CRAN (#97)

* Fix indexing into connections with quoted fields (#119)

* Move example files for `vroom()` out of `\dontshow{}`.

* Fix integer overflow with very large files (#116, #119)

* Fix missing columns and windows newlines (#114)

* Fix encoding of column names (#113, #115)

* Throw an error message when writing a zip file, which is not supported (@metaOO, #145)

* Default message output from `vroom()` now uses `Rows` and `Cols` (@meta00, #140)


# vroom 1.0.1

## New Features

* `vroom_lines()` function added, to (lazily) read lines from a file into a
  character vector (#90).

## Minor improvements and fixes

* Fix for a hang on Windows caused by a race condition in the progress bar (#98)

* Remove accidental runtime dependency on testthat (#104)

* Fix to actually return non-Altrep character columns on R 3.2, 3.3 and 3.4.

* Disable colors in the progress bar when running in RStudio, to work around an
  issue where the progress bar would be garbled (https://github.com/rstudio/rstudio/issues/4777)

* Fix for UBSAN issues reported by CRAN (#97)

* Fix for rchk issues reported by CRAN (#94)

* The progress bar now only updates every 10 milliseconds.

* Getting started vignette index entry now more informative (#92)

# vroom 1.0.0

* Initial release

* Added a `NEWS.md` file to track changes to the package.
