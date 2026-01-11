library(vroom)

# Create a test fixture with comment lines at the top
# Used to test fix for https://github.com/tidyverse/vroom/issues/400
#
# The file must also be large enough to trigger the bug, which involves how
# gzcon() interacts with vroom's chunked connection reading. Small files
# that fit in a single buffer don't trigger it.

withr::local_dir("inst/extdata/")

mt <- vroom(vroom_example("mtcars.csv"), col_types = list())

# Replicate mtcars to make the file large enough to trigger the bug
# 128 KiB is the default connection buffer size (VROOM_CONNECTION_SIZE)
mt_large <- mt[rep(seq_len(nrow(mt)), 200), ]

# Create a version with comment lines at the top
target <- "mtcars-large-comment.csv.gz"

con <- gzfile(target, "wb")
writeLines(
  c(
    "# This is a comment line",
    "# Another comment line",
    "# Yet another comment"
  ),
  con
)
vroom_write(mt_large, con, delim = ",")
close(con)

# Verify the uncompressed size exceeds the 128 KiB connection buffer
# (1 << 17 = 131072 bytes; see VROOM_CONNECTION_SIZE in src/index_collection.cc)
tf <- withr::local_tempfile(fileext = ".csv")
vroom_write(mt_large, tf, delim = ",")
uncompressed_kib <- file.size(tf) / 1024
stopifnot(uncompressed_kib > 128)
message(
  "Uncompressed size: ",
  round(uncompressed_kib),
  " KiB (must be > 128 KiB)"
)

# Verify the file can be read back correctly
result <- vroom(target, comment = "#", show_col_types = FALSE)
stopifnot(nrow(result) == nrow(mt_large))
stopifnot(identical(names(result), names(mt_large)))

withr::deferred_run()
