# Write the taxi data
file <- "~/data/trip_fare_1.tsv"

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

f <- tempfile(fileext = ".gz")

compressed <- bench::workout(description = c("write.delim", "readr", "vroom"), {
  { con <- gzfile(tempfile(), "wb");  write.table(data, con, sep = "\t", quote = FALSE, row.names = FALSE); close(con) }
  readr::write_tsv(data, tempfile(fileext = ".gz"))
  vroom_write(data, tempfile(fileext = ".gz"), delim = "\t")
})

compressed$op = "gzip"

multithreaded <- bench::workout(description = c("write.delim", "readr", "vroom"), {
  { con <- pipe(glue::glue('pigz > {tempfile(fileext = ".gz")}'), "wb");  write.table(data, con, sep = "\t", quote = FALSE, row.names = FALSE); close(con) }
  readr::write_tsv(data, pipe(glue::glue('pigz > {tempfile(fileext = ".gz")}')))
  vroom_write(data, pipe(glue::glue('pigz > {tempfile(fileext = ".gz")}')), delim = "\t")
})

multithreaded$op = "multithreaded gzip"


library(purrr)
library(tidyr)
library(dplyr)
library(forcats)
library(bench)

tm_df <- do.call(rbind, list(uncompressed, compressed, multithreaded)) %>%
  mutate(exprs = as.character(exprs)) %>%
  rename(package = exprs) %>%
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
