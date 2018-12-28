path <- "~/data/trip_fare_1.tsv"

vroom_base <- function(file) {
  library(vroom)
  list(
    bench::system_time(x <- vroom(file)),
    bench::system_time(print(x)),
    bench::system_time(head(x)),
    bench::system_time(tail(x)),
    bench::system_time(x[sample(NROW(x), 100), ]),
    bench::system_time(x[x$payment_type == "UNK", ])
  )
}

vroom_dplyr <- function(file) {
  library(vroom)
  library(dplyr)
  list(
    bench::system_time(x <- vroom(file)),
    bench::system_time(print(x)),
    bench::system_time(head(x)),
    bench::system_time(tail(x)),
    bench::system_time(sample_n(x, 100)),
    bench::system_time(filter(x, payment_type == "UNK"))
  )
}

data.table <- function(file) {
  library(data.table)
  list(
    bench::system_time(x <- data.table::fread(file)),
    bench::system_time(print(x)),
    bench::system_time(head(x)),
    bench::system_time(tail(x)),
    bench::system_time(x[sample(NROW(x), 100), ]),
    bench::system_time(x[x$payment_type == "UNK", ])
  )
}

readr <- function(file) {
  library(readr)
  library(dplyr)
  list(
    bench::system_time(x <- read_tsv(file)),
    bench::system_time(print(x)),
    bench::system_time(head(x)),
    bench::system_time(tail(x)),
    bench::system_time(sample_n(x, 100)),
    bench::system_time(filter(x, payment_type == "UNK"))
  )
}

read.delim <- function(file) {
  list(
    bench::system_time(x <- read.delim(file)),
    bench::system_time(print(head(x, 25))),
    bench::system_time(head(x)),
    bench::system_time(tail(x)),
    bench::system_time(x[sample(NROW(x), 100), ]),
    bench::system_time(x[x$payment_type == "UNK", ])
  )
}

times <- list(
  vroom_base = callr::r(vroom_base, list(file = path)),
  vroom_dplyr = callr::r(vroom_dplyr, list(file = path)),
  data.table = callr::r(data.table, list(file = path)),
  readr = callr::r(readr, list(file = path)),
  read.delim = callr::r(read.delim, list(file = path))
)

library(purrr)
library(tidyr)
library(dplyr)
library(forcats)

tm_df <- map_dfr(times, function(x) {
  ops <- c("read", "print", "head", "tail", "sample", "filter")
  tibble::tibble(
    op = factor(ops, levels = ops),
    process = map_dbl(x, "process"),
    real = map_dbl(x, "real")
  )
  }, .id = "package") %>%
  mutate(
    package = fct_inorder(package),
  ) %>%
  gather(type, time, -package, -op) %>%
  mutate(size = file.size("~/data/trip_fare_1.tsv"))

saveRDS(tm_df, here::here("bench", "timings.Rds"))
