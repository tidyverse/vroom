file <- commandArgs(trailingOnly = TRUE)[[1]]
desc <- c("setup", "read", "print", "head", "tail", "sample", "filter", "aggregate")

vroom_base <- function(file, desc) {
  bench::workout(description = desc,
    {
      library(vroom)
      x <- vroom(file, col_types = c(pickup_datetime = "c"), trim_ws = FALSE, quote = "", escape_double = FALSE, na = character())
      print(x)
      a <- head(x)
      b <- tail(x)
      c <- x[sample(NROW(x), 100), ]
      d <- x[x$payment_type == "UNK", ]
      e <- tapply(x$tip_amount, x$payment_type, mean)
    }
  )
}

`vroom (full altrep)_base` <- function(file, desc) {
  bench::workout(description = desc,
    {
    ({library(vroom)})
      x <- vroom(file, col_types = c(pickup_datetime = "c"), trim_ws = FALSE, quote = "", escape_double = FALSE, na = character(), altrep_opts = TRUE)
      print(x)
      a <- head(x)
      b <- tail(x)
      c <- x[sample(NROW(x), 100), ]
      d <- x[x$payment_type == "UNK", ]
      e <- tapply(x$tip_amount, x$payment_type, mean)
    }
  )
}

`vroom (no altrep)_base` <- function(file, desc) {
  bench::workout(description = desc,
    {
    ({library(vroom)})
      x <- vroom(file, col_types = c(pickup_datetime = "c"), trim_ws = FALSE, quote = "", escape_double = FALSE, na = character(), altrep_opts = FALSE)
      print(x)
      a <- head(x)
      b <- tail(x)
      c <- x[sample(NROW(x), 100), ]
      d <- x[x$payment_type == "UNK", ]
      e <- tapply(x$tip_amount, x$payment_type, mean)
    }
  )
}

vroom_dplyr <- function(file, desc) {
  bench::workout(description = desc,
    {
      ({ library(vroom); library(dplyr) })
      x <- vroom(file, col_types = c(pickup_datetime = "c"), trim_ws = FALSE, quote = "", escape_double = FALSE, na = character())
      print(x)
      a <- head(x)
      b <- tail(x)
      c <- sample_n(x, 100)
      d <- filter(x, payment_type == "UNK")
      e <- group_by(x, payment_type) %>% summarise(avg_tip = mean(tip_amount))
    }
  )
}

`vroom (full altrep)_dplyr` <- function(file, desc) {
  bench::workout(description = desc,
    {
      ({library(vroom); library(dplyr)})
      x <- vroom(file, col_types = c(pickup_datetime = "c"), trim_ws = FALSE, quote = "", escape_double = FALSE, na = character(), altrep_opts = TRUE)
      print(x)
      a <- head(x)
      b <- tail(x)
      c <- sample_n(x, 100)
      d <- filter(x, payment_type == "UNK")
      e <- group_by(x, payment_type) %>% summarise(avg_tip = mean(tip_amount))
    }
  )
}

`vroom (no altrep)_dplyr` <- function(file, desc) {
  bench::workout(description = desc,
    {
      ({library(vroom); library(dplyr)})
      x <- vroom(file, col_types = c(pickup_datetime = "c"), trim_ws = FALSE, quote = "", escape_double = FALSE, na = character(), altrep_opts = FALSE)
      print(x)
      a <- head(x)
      b <- tail(x)
      c <- sample_n(x, 100)
      d <- filter(x, payment_type == "UNK")
      e <- group_by(x, payment_type) %>% summarise(avg_tip = mean(tip_amount))
    }
  )
}

data.table <- function(file, desc) {
  bench::workout(description = desc,
    {
      library(data.table)
      x <- fread(file, sep = ",", quote = "", strip.white = FALSE, na.strings = NULL)
      print(x)
      a <- head(x)
      b <- tail(x)
      c <- x[sample(NROW(x), 100), ]
      d <- x[payment_type == "UNK", ]
      e <- x[ , .(mean(tip_amount)), by = payment_type]
    }
  )
}

readr <- function(file, desc) {
  bench::workout(description = desc,
    {
      ({ library(readr); library(dplyr) })
      x <- read_csv(file, col_types = c(pickup_datetime = "c"), quote = "", trim_ws = FALSE, na = character())
      print(x)
      a <- head(x)
      b <- tail(x)
      c <- sample_n(x, 100)
      d <- filter(x, payment_type == "UNK")
      e <- group_by(x, payment_type) %>% summarise(avg_tip = mean(tip_amount))
    }
  )
}

read.delim <- function(file, desc) {
  bench::workout(description = desc,
    {
      ({})
      x <- read.delim(file, sep = ",", quote = "", na.strings = NULL, stringsAsFactors = FALSE)
      print(head(x, 10))
      a <- head(x)
      b <- tail(x)
      c <- x[sample(NROW(x), 100), ]
      d <- x[x$payment_type == "UNK", ]
      e <- tapply(x$tip_amount, x$payment_type, mean)
    }
  )
}

times <- list(
  vroom_base = callr::r(vroom_base, list(file, desc)),
  `vroom (full altrep)_base` = callr::r(`vroom (full altrep)_base`, list(file, desc)),
  `vroom (no altrep)_base` = callr::r(`vroom (no altrep)_base`, list(file, desc)),
  vroom_dplyr = callr::r(vroom_dplyr, list(file, desc)),
  `vroom (full altrep)_dplyr` = callr::r(`vroom (full altrep)_dplyr`, list(file, desc)),
  `vroom (no altrep)_dplyr` = callr::r(`vroom (no altrep)_dplyr`, list(file, desc)),
  data.table = callr::r(data.table, list(file, desc)),
  readr = callr::r(readr, list(file, desc)),
  read.delim = callr::r(read.delim, list(file, desc))
)

library(purrr)
library(tidyr)
library(dplyr)
library(forcats)
library(bench)

data <- vroom::vroom(file)

tm_df <- map_dfr(times, function(x) {
  tibble::tibble(
    op = factor(x$exprs, levels = desc, ordered = TRUE),
    process = as.numeric(x$process),
    real = as.numeric(x$real),
  )
  }, .id = "package") %>%
  gather(type, time, -package, -op) %>%
  mutate(size = file.size(file),
    rows = nrow(data),
    cols = ncol(data)
  )

vroom::vroom_write(
  tm_df,
  here::here("inst", "bench", "taxi-times.tsv"),
  delim = "\t")

vroom::vroom_write(
  sessioninfo::package_info(c("vroom", "readr", "dplyr", "data.table", "base"), dependencies = FALSE, include_base = TRUE),
  here::here("inst", "bench", "sessioninfo.tsv"),
  delim = "\t"
)
