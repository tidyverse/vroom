This minor release is at the request of CRAN, but also incorporates new features and bug fixes. The specific request from CRAN:

> Specifically, please see the WARNING in the compiled code check about
using non-API entry points which may be removed soon. 
>
> Please correct before 2026-02-01 to safely retain your package on CRAN.

vroom takes the recommended approach to discontinuing the use of ‘ATTRIB’, ‘SETLENGTH’, ‘SET_TRUELENGTH’.

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
