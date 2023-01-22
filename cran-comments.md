This patch release removes all usage of sprintf() and is being done under a deadline of 2023-01-23.

## revdepcheck results

We checked 33 reverse dependencies, comparing R CMD check results across CRAN and dev versions of this package.

 * We saw 0 new problems
 * We failed to check 2 packages

Issues with CRAN packages are summarised below.

### Failed to check

* elbird (NA). This package failed to install. Possibly due to a dependency on
  git?
* INSPECTumours (NA). This package failed to install, due to a dependency on
  rstan, which apparently did not install successfully in my revdep
  environment.
