# Write the taxi data
file <- commandArgs(trailingOnly = TRUE)[[1]]

library(vroom)

data <- vroom(file, col_types = c(pickup_datetime = "c"))
vroom:::vroom_materialize(data)

file <- tempfile()

desc <- c("write.delim", "readr", "vroom", "data.table")

uncompressed <- bench::workout(description = desc, {
  write.table(data, tempfile(), sep = "\t", quote = FALSE, row.names = FALSE)
  readr::write_tsv(data, tempfile())
  vroom_write(data, file, delim = "\t")
  data.table::fwrite(data, tempfile(), sep = "\t")
})

uncompressed$op = "uncompressed"

xz <- bench::workout(description = desc, {
  ({ con <- xzfile(tempfile(fileext = ".xz"), "wb");  write.table(data, con, sep = "\t", quote = FALSE, row.names = FALSE); close(con) })
  readr::write_tsv(data, tempfile(fileext = ".xz"))
  vroom_write(data, tempfile(fileext = ".xz"), delim = "\t")
  NULL # unsupported
})

xz$op <- "xz"

gzip <- bench::workout(description = desc, {
  ({ con <- gzfile(tempfile(), "wb");  write.table(data, con, sep = "\t", quote = FALSE, row.names = FALSE); close(con) })
  readr::write_tsv(data, tempfile(fileext = ".gz"))
  vroom_write(data, tempfile(fileext = ".gz"), delim = "\t")
  NULL # data.table::fwrite(data, tempfile(fileext = ".gz"), sep = "\t", nThread = 1)
})

gzip$op = "gzip"

multithreaded_gzip <- bench::workout(description = desc, {
  ({ con <- pipe(glue::glue('pigz > {tempfile(fileext = ".gz")}'), "wb");  write.table(data, con, sep = "\t", quote = FALSE, row.names = FALSE); close(con) })
  readr::write_tsv(data, pipe(glue::glue('pigz > {tempfile(fileext = ".gz")}')))
  vroom_write(data, pipe(glue::glue('pigz > {tempfile(fileext = ".gz")}')), delim = "\t")
  NULL # data.table::fwrite(data, tempfile(fileext = ".gz"), sep = "\t")
})

multithreaded_gzip$op = "multithreaded gzip"

zstandard <- bench::workout(description = desc, {
  ({ con <- pipe(glue::glue('zstd > {tempfile(fileext = ".zst")}'), "wb");  write.table(data, con, sep = "\t", quote = FALSE, row.names = FALSE); close(con) })
  readr::write_tsv(data, pipe(glue::glue('zstd > {tempfile(fileext = ".zst")}')))
  vroom_write(data, pipe(glue::glue('zstd > {tempfile(fileext = ".zst")}')), delim = "\t")
  NULL # unsupported
})

zstandard$op = "zstandard"

library(purrr)
library(tidyr)
library(dplyr)
library(forcats)
library(bench)

tm_df <- do.call(rbind, list(uncompressed, xz, gzip, multithreaded_gzip, zstandard)) %>%
  mutate(exprs = as.character(exprs)) %>%
  rename(package = exprs) %>%
  filter(package != "data.table" | op == "uncompressed") %>%
  gather(type, time, -package, -op) %>%
  mutate(
    time = as.numeric(time),
    size = file.size(file),
    rows = nrow(data),
    cols = ncol(data)
  ) %>%
  select(package, op, type, time, size, rows, cols)

vroom::vroom_write(
  tm_df,
  here::here("inst", "bench", "taxi_writing-times.tsv"),
  delim = "\t")
