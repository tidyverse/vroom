This small patch release is at the request of Kurt Hornik:

> Specifically, plase see the URLs with

>  Message: Invalid URL: missing authority part

> in the CRAN incoming feasibility check.

vroom also has a NOTE about non-API calls to R which I know I need to address but I will do that in a near-term minor or major release, where I will also do full reverse dependency checks (involving vroom and readr).

My near term goal is to just fix the bad URL in vroom's README.

## revdepcheck results

THESE RESULTS PERTAIN TO vroom v1.6.5!

We checked 40 reverse dependencies, comparing R CMD check results across CRAN and dev versions of this package.

 * We saw 0 new problems
 * We failed to check 0 packages
