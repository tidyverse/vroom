This patch release (version 1.7.1) is at the request of CRAN and is associated with a deadline of 2026-04-13:

* Removing use of  `Rf_findVarInFrame` and `R_NamespaceRegistry`
* Preparing for compatibility with clang 22

The results below pertain to the recent minor release (version 1.7.0 on 2026-01-27). I did not rerun reverse dependency checks given the internal nature of the changes.

## revdepcheck results

We checked 49 reverse dependencies, comparing R CMD check results across CRAN and dev versions of this package.

 * We saw 3 new problems
 * We failed to check 0 packages

Issues with CRAN packages are summarised below.

### New problems
(This reports the first line of each new failure)

* arkdb
  checking tests ... ERROR
  arkdb is using a now-removed argument that was deprecated >4 years ago. I made
  a PR to fix on 2025-11-19. That's been merged but not released on CRAN yet.

* FORTLS
  checking re-building of vignette outputs ... ERROR
  FORTLS is using a now-removed argument that was deprecated >4 years ago. When
  I ran a revdep check in December 2025, FORTLS had been removed from CRAN and
  did not turn up. It reappeared on CRAN a few days ago on 2026-01-19 and just
  appeared in my final revdepchecks. I have already opened a PR to fix its
  usage.

* readr
  checking tests ... ERROR
  I am also the maintainer of readr, so this is a self-goal. readr has an
  unfortunate test of the exact form of an error message emitted by vroom and
  that message has changed. I will fix this test soon in a readr release. There
  is no breakage for anyone who is using readr or vroom.
