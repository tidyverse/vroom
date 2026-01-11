library(vroom)

# Create a test fixture with comment lines at the top
# Used to test fix for https://github.com/tidyverse/vroom/issues/400

withr::local_dir("inst/extdata/")

mt <- vroom(vroom_example("mtcars.csv"), col_types = list())

# Create a version with comment lines at the top
target <- "mtcars-comment.csv.gz"

con <- gzfile(target, "wb")
writeLines(
  c(
    "# This is a comment line",
    "# Another comment line",
    "# Yet another comment"
  ),
  con
)
vroom_write(mt, con, delim = ",")
close(con)

# Verify the file can be read back correctly
result <- vroom(target, comment = "#", show_col_types = FALSE)
stopifnot(nrow(result) == nrow(mt))
stopifnot(identical(names(result), names(mt)))

withr::deferred_run()
