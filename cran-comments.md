This release should fix the errors on Solaris, which were due to unintentionally
leaking file handles.

## Test environments
* local OS X install, R 3.6.1
* ubuntu 14.04 (on GitHub Actions), R 3.6.1
* win-builder (devel and release)

## R CMD check results

0 errors | 0 warnings | 0 note

## Reverse dependencies
I checked all 5 reverse dependencies, there were no regressions with this
version.
