path <- "~/data/trip_fare_1.tsv"
desc <- c("library", "read", "print", "head", "tail", "sample", "filter")

watch <- function(expr, description = NULL) {
  expr <- rlang::enquo(expr)
  env <- rlang::quo_get_env(expr)
  exprs <- as.list(rlang::quo_get_expr(expr)[-1])

  if (is.null(description)) {
    description <- names(exprs)
  }

  out <- list(
    exprs = new_bench_expr(exprs, description),
    process = numeric(length(exprs)),
    real = numeric(length(exprs))
  )

  for (i in seq_along(exprs)) {
    res <- rlang::eval_bare(bench::system_time(!!exprs[[i]]), env)
    out[[2]][[i]] <- res[[1]]
    out[[3]][[i]] <- res[[2]]
  }

  out[[2]] <- bench::as_bench_time(out[[2]])
  out[[3]] <- bench::as_bench_time(out[[3]])

  tibble::as_tibble(out)
}

vroom_base <- function(file, desc) {
  watch(description = desc,
    {
      library(vroom)
      x <- vroom(file, col_types = c(pickup_datetime = "c"), quote = "", escape_double = FALSE, na = character())
      print(x)
      head(x)
      tail(x)
      x[sample(NROW(x), 100), ]
      x[x$payment_type == "UNK", ]
    }
  )
}

vroom_dplyr <- function(file, desc) {
  watch(description = desc,
    {
      { library(vroom); library(dplyr) }
      x <- vroom(file, col_types = c(pickup_datetime = "c"), quote = "", escape_double = FALSE, na = character())
      print(x)
      head(x)
      tail(x)
      sample_n(x, 100)
      filter(x, payment_type == "UNK")
    }
  )
}

data.table <- function(file, desc) {
  watch(description = desc,
    {
      library(data.table)
      x <- fread(file, sep = "\t", quote = "", strip.white = FALSE, na.strings = NULL)
      print(x)
      head(x)
      tail(x)
      x[sample(NROW(x), 100), ]
      x[payment_type == "UNK", ]
    }
  )
}

readr <- function(file, desc) {
  watch(description = desc,
    {
    { library(readr); library(dplyr) }
      x <- read_tsv(file, col_types = c(pickup_datetime = "c"), quote = "", trim_ws = FALSE, na = character())
      print(x)
      head(x)
      tail(x)
      sample_n(x, 100)
      filter(x, payment_type == "UNK")
    }
  )
}

read.delim <- function(file, desc) {
  watch(description = desc,
    {
      x <- read.delim(file, quote = "", na.strings = NULL)
      print(x)
      head(x)
      tail(x)
      x[sample(NROW(x), 100), ]
      x[x$payment_type == "UNK", ]
    }
  )
}

times <- list(
  vroom_base = callr::r(vroom_base, list(file = path, desc = desc)),
  vroom_dplyr = callr::r(vroom_dplyr, list(file = path, desc = desc)),
  data.table = callr::r(data.table, list(file = path, desc = desc)),
  readr = callr::r(readr, list(file = path, desc = desc)),
  read.delim = callr::r(read.delim, list(file = path, desc = desc))
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

readr::write_tsv(tm_df, here::here("inst", "bench", "timings.tsv"))
readr::write_tsv(sessioninfo::package_info(c("vroom", "readr", "dplyr", "data.table", "base"), dependencies = FALSE, include_base = TRUE),
  here::here("inst", "bench", "sessioninfo.tsv")
)
