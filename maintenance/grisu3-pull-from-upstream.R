# Pull grisu3 files from upstream MathGeoLib repository
# https://github.com/juj/MathGeoLib
#
# After running this script, apply patches from maintenance/patches/
# to get the versions vroom actually uses. See 02_apply-grisu3-patches.txt

library(usethis)
library(gh)

# Upstream: https://github.com/juj/MathGeoLib/tree/master/src/Math
owner <- "juj"
repo <- "MathGeoLib"
repo_spec <- paste0(owner, "/", repo)

# Get current HEAD SHA
gh_result <- gh(
  "/repos/{owner}/{repo}/commits/{ref}",
  owner = owner,
  repo = repo,
  ref = "HEAD"
)
upstream_sha <- gh_result$sha
upstream_date <- as.Date(gh_result$commit$author$date)

message(sprintf(
  "Upstream SHA: %s (%s)",
  substr(upstream_sha, 1, 7),
  upstream_date
))
# Upstream SHA: 55053da (2023-01-21)

# Download grisu3 files to maintenance/ for patching
use_github_file(
  repo_spec = repo_spec,
  path = "src/Math/grisu3.h",
  save_as = "maintenance/grisu3.h",
  ref = upstream_sha
)

use_github_file(
  repo_spec = repo_spec,
  path = "src/Math/grisu3.c",
  save_as = "maintenance/grisu3.c",
  ref = upstream_sha
)

writeLines(
  c(
    sprintf("grisu3 vendored from: https://github.com/%s", repo_spec),
    sprintf("Commit: %s", upstream_sha),
    sprintf("Date: %s", upstream_date),
    sprintf(
      "Permalink: https://github.com/%s/tree/%s/src/Math",
      repo_spec,
      upstream_sha
    )
  ),
  "maintenance/grisu3-upstream-info.txt"
)
