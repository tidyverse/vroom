library(vroom)

# Create a test fixture that triggers https://github.com/tidyverse/vroom/issues/400
#
# The key insight is that the original EPSS file is a CONCATENATED gzip archive -
# multiple gzip members joined together. gzcon() only reads the first member,
# while gzfile() reads all of them. This is also related to issue #553.
#
# To trigger the bug, we create a concatenated gzip: the comment line is one
# gzip member, and the data is another gzip member.

withr::local_dir("inst/extdata/")

mt <- vroom(vroom_example("mtcars.csv"), col_types = list())

target <- "mtcars-concatenated.csv.gz"

# Create a CONCATENATED gzip file (multiple gzip members)
# This mimics the structure of files like the EPSS data that revealed the bug

comment_gz <- withr::local_tempfile(fileext = ".gz")
data_csv <- withr::local_tempfile(fileext = ".csv")
data_gz <- withr::local_tempfile(fileext = ".gz")

# Gzip the comment line (first gzip member)
con <- gzfile(comment_gz, "wb")
writeLines(
  "#model_version:v2022.01.01,score_date:2022-02-04T00:00:00+0000",
  con
)
close(con)

# Write the data to csv, then gzip it (second gzip member)
vroom_write(mt, data_csv, delim = ",")
system2("gzip", c("-k", data_csv)) # creates data_csv.gz alongside data_csv

# Concatenate the raw gzip bytes into the target file
writeBin(
  c(
    readBin(comment_gz, "raw", file.size(comment_gz)),
    readBin(paste0(data_csv, ".gz"), "raw", file.size(paste0(data_csv, ".gz")))
  ),
  target
)

# Verify this is indeed a concatenated gzip that gzcon() can't handle
# but gzfile() can
con_gzcon <- gzcon(file(target, "rb"))
lines_gzcon <- readLines(con_gzcon, n = 5)
close(con_gzcon)
stopifnot(length(lines_gzcon) == 1) # gzcon only sees first member
message("gzcon() sees only ", length(lines_gzcon), " line (expected: 1)")

con_gzfile <- gzfile(target, "r")
lines_gzfile <- readLines(con_gzfile, n = 5)
close(con_gzfile)
stopifnot(length(lines_gzfile) == 5) # gzfile sees all members
message("gzfile() sees ", length(lines_gzfile), " lines (expected: 5)")

# Verify the file can be read back correctly with vroom
result <- vroom(target, comment = "#", show_col_types = FALSE)
stopifnot(nrow(result) == nrow(mt))
stopifnot(identical(names(result), names(mt)))
message("vroom read ", nrow(result), " rows successfully")

withr::deferred_run()
