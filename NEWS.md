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
