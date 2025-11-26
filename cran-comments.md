This patch release is at the request of CRAN:

> Specifically, please see the WARNING in the r-devel compiled code check
about using non-API entry points which may be removed soon.  These have
replacements documented in "Some API replacements for non-API entry
points" of "Writing R Extensions": please change to use these
replacements.

vroom no longer uses `STDVEC_DATAPTR()` and takes the recommended approach for phasing out usage of `DATAPTR()`.

## revdepcheck results

THESE RESULTS PERTAIN TO vroom v1.6.5!

We checked 40 reverse dependencies, comparing R CMD check results across CRAN and dev versions of this package.

 * We saw 0 new problems
 * We failed to check 0 packages
