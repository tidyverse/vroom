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
