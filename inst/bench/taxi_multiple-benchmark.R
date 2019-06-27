library(vroom)
files <- commandArgs(trailingOnly = TRUE)
desc <- c("setup", "read", "print", "head", "tail", "sample", "filter", "aggregate")

`vroom (full altrep)_dplyr` <- function(files, desc) {
  bench::workout(description = desc,
    {
      ({library(vroom); library(dplyr)})
      x <- vroom(files, id = "path", col_types = c(pickup_datetime = "c"), trim_ws = FALSE, quote = "", escape_double = FALSE, na = character(), altrep_opts = TRUE)
      print(x)
      a <- head(x)
      b <- tail(x)
      c <- sample_n(x, 100)
      d <- filter(x, payment_type == "UNK")
      e <- group_by(x, payment_type) %>% summarise(avg_tip = mean(tip_amount))
    }
  )
}

`vroom (no altrep)_dplyr` <- function(files, desc) {
  bench::workout(description = desc,
    {
      ({library(vroom); library(dplyr)})
      x <- vroom(files, id = "path", col_types = c(pickup_datetime = "c"), trim_ws = FALSE, quote = "", escape_double = FALSE, na = character(), altrep_opts = FALSE)
      print(x)
      a <- head(x)
      b <- tail(x)
      c <- sample_n(x, 100)
      d <- filter(x, payment_type == "UNK")
      e <- group_by(x, payment_type) %>% summarise(avg_tip = mean(tip_amount))
    }
  )
}

data.table <- function(files, desc) {
  bench::workout(description = desc,
    {
      library(data.table)
      x <- rbindlist(idcol = "path",
        lapply(stats::setNames(files, files), fread, sep = ",", quote = "", strip.white = FALSE, na.strings = NULL)
      )
      print(x)
      a <- head(x)
      b <- tail(x)
      c <- x[sample(NROW(x), 100), ]
      d <- x[payment_type == "UNK", ]
      e <- x[ , .(mean(tip_amount)), by = payment_type]
    }
  )
}

readr <- function(files, desc) {
  bench::workout(description = desc,
    {
      ({ library(readr); library(dplyr); library(purrr) })
      x <- map_dfr(set_names(files), .id = "path",
        ~ read_csv(.x, col_types = c(pickup_datetime = "c"), quote = "", trim_ws = FALSE, na = character())
      )
      print(x)
      a <- head(x)
      b <- tail(x)
      c <- sample_n(x, 100)
      d <- filter(x, payment_type == "UNK")
      e <- group_by(x, payment_type) %>% summarise(avg_tip = mean(tip_amount))
    }
  )
}

read.delim <- function(files, desc) {
  bench::workout(description = desc,
    {
      ({})
      {x <- do.call(rbind.data.frame,
        c(lapply(stats::setNames(files, files), read.delim, sep = ",", quote = "", na.strings = NULL, stringsAsFactors = FALSE), stringsAsFactors = FALSE, make.row.names = TRUE)
      )
      # need to make the new column out of the munged row names
      x$path <- sub("[.]\\d+$", "", rownames(x))
      rownames(x) <- NULL
      }
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
  `vroom (full altrep)_dplyr` = callr::r(`vroom (full altrep)_dplyr`, list(files, desc)),
  `vroom (no altrep)_dplyr` = callr::r(`vroom (no altrep)_dplyr`, list(files, desc)),
  data.table = callr::r(data.table, list(files, desc)),
  readr = callr::r(readr, list(files, desc)),
  read.delim = callr::r(read.delim, list(files, desc))
)

library(purrr)
library(tidyr)
library(dplyr)
library(forcats)
library(bench)

data <- vroom::vroom(files, id = "path")

tm_df <- map_dfr(times, function(x) {
  tibble::tibble(
    op = factor(x$exprs, levels = desc, ordered = TRUE),
    process = as.numeric(x$process),
    real = as.numeric(x$real),
  )
  }, .id = "package") %>%
  gather(type, time, -package, -op) %>%
  mutate(size = sum(file.size(files)),
    rows = nrow(data),
    cols = ncol(data)
  )

vroom::vroom_write(
  tm_df,
  here::here("inst", "bench", "taxi_multiple-times.tsv"),
  delim = "\t")

vroom::vroom_write(
  sessioninfo::package_info(c("vroom", "readr", "dplyr", "data.table", "base"), dependencies = FALSE, include_base = TRUE),
  here::here("inst", "bench", "sessioninfo.tsv"),
  delim = "\t"
)

