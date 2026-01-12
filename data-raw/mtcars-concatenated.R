library(vroom)

withr::local_dir("inst/extdata/")

# Create a test fixture that triggers https://github.com/tidyverse/vroom/issues/400
#
# The key insight is that the original EPSS file is a CONCATENATED gzip archive -
# multiple gzip members joined together. gzcon() only reads the first member,
# while gzfile() reads all of them. This is also related to issue #553.
#
# To trigger the bug, we create a concatenated gzip: the comment line is one
# gzip member, and the data is another gzip member.

comment_gz <- withr::local_tempfile(fileext = ".gz")
data_gz <- withr::local_tempfile(fileext = ".gz")
target <- "mtcars-concatenated.csv.gz"


# gzip the comment line (first gzip member)
# exact line is taken from OP's example in #400
con <- gzfile(comment_gz, "wb")
writeLines(
  "#model_version:v2022.01.01,score_date:2022-02-04T00:00:00+0000",
  con
)
close(con)

# gzip the mtcars data (second gzip member)
con <- gzfile(data_gz, "wb")
vroom_example("mtcars.csv") |>
  vroom(, col_types = list()) |>
  vroom_write(con, delim = ",")
close(con)

# Concatenate the raw gzip bytes into the target file
writeBin(
  c(
    readBin(comment_gz, "raw", file.size(comment_gz)),
    readBin(data_gz, "raw", file.size(data_gz))
  ),
  target
)

# Verify this is indeed a concatenated gzip that gzcon() can't handle
# but gzfile() can
con_gzcon <- gzcon(file(target, "rb"))
(lines_gzcon <- readLines(con_gzcon, n = 5))
close(con_gzcon)
stopifnot(length(lines_gzcon) == 1) # gzcon only sees first member
message("gzcon() sees ", length(lines_gzcon), " line")

con_gzfile <- gzfile(target, "r")
(lines_gzfile <- readLines(con_gzfile, n = 5))
close(con_gzfile)
stopifnot(length(lines_gzfile) == 5) # gzfile sees all members
message("gzfile() sees ", length(lines_gzfile), " lines")

# Verify the file can be read back correctly with vroom
result <- vroom(target, comment = "#", show_col_types = FALSE)
stopifnot(nrow(result) == nrow(mt))
stopifnot(identical(names(result), names(mt)))
message("vroom read ", nrow(result), " rows successfully")

withr::deferred_run()
