## revdepcheck results

We checked 33 reverse dependencies, comparing R CMD check results across CRAN and dev versions of this package.

 * We saw 1 new problem
 * We failed to check 1 package

Issues with CRAN packages are summarised below.

### New problems

* readr
  checking tests ... ERROR
  
This is an expected test failure in readr, which I also maintain and which will be updated once vroom is released on CRAN. We have changed a behaviour in vroom (made it more correct) and this temporarily causes a test in readr to fail. We have to submit vroom first, then readr.

### Failed to check

* elbird (NA). This package failed to install. Possibly due to a dependency on
  git?
