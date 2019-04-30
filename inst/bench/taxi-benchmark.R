file <- "~/data/trip_fare_1.tsv"
desc <- c("setup", "read", "print", "head", "tail", "sample", "filter", "aggregate")

`vroom (full altrep)_base` <- function(file, desc) {
  bench::workout(description = desc,
    {
    {library(vroom); Sys.setenv("VROOM_USE_ALTREP_NUMERICS" = "true") }
      x <- vroom(file, col_types = c(pickup_datetime = "c"), quote = "", escape_double = FALSE, na = character())
      print(x)
      head(x)
      tail(x)
      x[sample(NROW(x), 100), ]
      x[x$payment_type == "UNK", ]
      tapply(x$tip_amount, x$payment_type, mean)
    }
  )
}

`vroom (full altrep)_dplyr` <- function(file, desc) {
  bench::workout(description = desc,
    {
      {library(vroom); library(dplyr); Sys.setenv("VROOM_USE_ALTREP_NUMERICS" = "true") }
      x <- vroom(file, col_types = c(pickup_datetime = "c"), quote = "", escape_double = FALSE, na = character())
      print(x)
      head(x)
      tail(x)
      sample_n(x, 100)
      filter(x, payment_type == "UNK")
      group_by(x, payment_type) %>% summarise(avg_tip = mean(tip_amount))
    }
  )
}

vroom_base <- function(file, desc) {
  bench::workout(description = desc,
    {
      library(vroom)
      x <- vroom(file, col_types = c(pickup_datetime = "c"), quote = "", escape_double = FALSE, na = character())
      print(x)
      head(x)
      tail(x)
      x[sample(NROW(x), 100), ]
      x[x$payment_type == "UNK", ]
      tapply(x$tip_amount, x$payment_type, mean)
    }
  )
}

vroom_dplyr <- function(file, desc) {
  bench::workout(description = desc,
    {
      { library(vroom); library(dplyr) }
      x <- vroom(file, col_types = c(pickup_datetime = "c"), quote = "", escape_double = FALSE, na = character())
      print(x)
      head(x)
      tail(x)
      sample_n(x, 100)
      filter(x, payment_type == "UNK")
      group_by(x, payment_type) %>% summarise(avg_tip = mean(tip_amount))
    }
  )
}

data.table <- function(file, desc) {
  bench::workout(description = desc,
    {
      library(data.table)
      x <- fread(file, sep = "\t", quote = "", strip.white = FALSE, na.strings = NULL)
      print(x)
      head(x)
      tail(x)
      x[sample(NROW(x), 100), ]
      x[payment_type == "UNK", ]
      x[ , .(mean(tip_amount)), by = payment_type]
    }
  )
}

readr <- function(file, desc) {
  bench::workout(description = desc,
    {
    { library(readr); library(dplyr) }
      x <- read_tsv(file, col_types = c(pickup_datetime = "c"), quote = "", trim_ws = FALSE, na = character())
      print(x)
      head(x)
      tail(x)
      sample_n(x, 100)
      filter(x, payment_type == "UNK")
      group_by(x, payment_type) %>% summarise(avg_tip = mean(tip_amount))
    }
  )
}

read.delim <- function(file, desc) {
  bench::workout(description = desc,
    {
      x <- read.delim(file, quote = "", na.strings = NULL)
      print(head(x, 10))
      head(x)
      tail(x)
      x[sample(NROW(x), 100), ]
      x[x$payment_type == "UNK", ]
      tapply(x$tip_amount, x$payment_type, mean)
    }
  )
}

times <- list(
  `vroom (full altrep)_base` = callr::r(`vroom (full altrep)_base`, list(file, desc)),
  `vroom (full altrep)_dplyr` = callr::r(`vroom (full altrep)_dplyr`, list(file, desc)),
  vroom_base = callr::r(vroom_base, list(file, desc)),
  vroom_dplyr = callr::r(vroom_dplyr, list(file, desc)),
  data.table = callr::r(data.table, list(file, desc)),
  readr = callr::r(readr, list(file, desc)),
  read.delim = callr::r(read.delim, list(file, desc))
)

library(purrr)
library(tidyr)
library(dplyr)
library(forcats)

tm_df <- map_dfr(times, function(x) {
  tibble::tibble(
    op = factor(x$exprs, levels = desc, ordered = TRUE),
    process = as.numeric(x$process),
    real = as.numeric(x$real),
  )
  }, .id = "package") %>%
  mutate(
    package = fct_inorder(package),
  ) %>%
  gather(type, time, -package, -op) %>%
  mutate(size = file.size("~/data/trip_fare_1.tsv"))

readr::write_tsv(tm_df, here::here("inst", "bench", "taxi-times.tsv"))
readr::write_tsv(sessioninfo::package_info(c("vroom", "readr", "dplyr", "data.table", "base"), dependencies = FALSE, include_base = TRUE),
  here::here("inst", "bench", "sessioninfo.tsv")
)
