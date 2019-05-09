# vroom (development version)

* Fix to actually return non-altrep character columns on R 3.2, 3.3 and 3.4.

* The progress bar now only updates every 10 milliseconds

* Disable colors in the progress bar when running in RStudio, so work around
  https://github.com/rstudio/rstudio/issues/4777

* Fix for a hang on Windows caused by a race condition in the progress bar (#98)

* Fix for UBSAN issues reported by CRAN (#97)

* Fix for rchk issues reported by CRAN (#94)

# vroom 1.0.0

* Initial release

* Added a `NEWS.md` file to track changes to the package.
