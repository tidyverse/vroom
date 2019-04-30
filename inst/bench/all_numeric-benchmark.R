rows <- 1e6
cols <- 25

set.seed(42)
RNGversion("3.5.3")

library(fs)

file <- path(tempdir(), "all-num.tsv")

data <- vroom::gen_tbl(rows, cols, col_types = strrep("d", cols))
vroom::vroom_write(data, file, "\t")

desc <- c("setup", "read", "print", "head", "tail", "sample", "filter", "aggregate")

`vroom (full altrep)_base` <- function(file, desc) {
  bench::workout(description = desc,
    {
    {library(vroom); Sys.setenv("VROOM_USE_ALTREP_NUMERICS" = "true") }
      x <- vroom(file, trim_ws = FALSE, quote = "", escape_double = FALSE, na = character())
      print(x)
      a <- head(x)
      b <- tail(x)
      c <- x[sample(NROW(x), 100), ]
      d <- x[x$X1 > 3, ]
      e <- tapply(x$X1, as.integer(x$X2), mean)
    }
  )
}

`vroom (full altrep)_dplyr` <- function(file, desc) {
  bench::workout(description = desc,
    {
      {library(vroom); library(dplyr); Sys.setenv("VROOM_USE_ALTREP_NUMERICS" = "true") }
      x <- vroom(file, trim_ws = FALSE, quote = "", escape_double = FALSE, na = character())
      print(x)
      a <- head(x)
      b <- tail(x)
      c <- sample_n(x, 100)
      d <- filter(x, X1 > 3)
      e <- group_by(x, as.integer(X2)) %>% summarise(avg_X1 = mean(X1))
    }
  )
}

vroom_base <- function(file, desc) {
  bench::workout(description = desc,
    {
      library(vroom)
      x <- vroom(file, trim_ws = FALSE, quote = "", escape_double = FALSE, na = character())
      print(x)
      a <- head(x)
      b <- tail(x)
      c <- x[sample(NROW(x), 100), ]
      d <- x[x$X1 > 3, ]
      e <- tapply(x$X1, as.integer(x$X2), mean)
    }
  )
}

vroom_dplyr <- function(file, desc) {
  bench::workout(description = desc,
    {
      { library(vroom); library(dplyr) }
      x <- vroom(file, trim_ws = FALSE, quote = "", escape_double = FALSE, na = character())
      print(x)
      a <- head(x)
      b <- tail(x)
      c <- sample_n(x, 100)
      d <- filter(x, X1 > 3)
      e <- group_by(x, as.integer(X2)) %>% summarise(avg_X1 = mean(X1))
    }
  )
}

data.table <- function(file, desc) {
  bench::workout(description = desc,
    {
      library(data.table)
      x <- fread(file, sep = "\t", quote = "", strip.white = FALSE, na.strings = NULL)
      print(x)
      a <- head(x)
      b <- tail(x)
      c <- x[sample(NROW(x), 100), ]
      d <- x[X1 > 3, ]
      e <- x[ , .(mean(X1)), by = as.integer(X2)]
    }
  )
}

readr <- function(file, desc) {
  bench::workout(description = desc,
    {
    { library(readr); library(dplyr) }
      x <- read_tsv(file, trim_ws = FALSE, quote = "", na = character())
      print(x)
      a <- head(x)
      b <- tail(x)
      c <- sample_n(x, 100)
      d <- filter(x, X1 > 3)
      e <- group_by(x, as.integer(X2)) %>% summarise(avg_X1 = mean(X1))
    }
  )
}

read.delim <- function(file, desc) {
  bench::workout(description = desc,
    {
      {}
      x <- read.delim(file, quote = "", na.strings = NULL, stringsAsFactors = FALSE)
      print(head(x, 10))
      a <- head(x)
      b <- tail(x)
      c <- x[sample(NROW(x), 100), ]
      d <- x[x$X1 > 3, ]
      e <- tapply(x$X1, as.integer(x$X2), mean)
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
library(bench)

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
  mutate(size = file.size(file),
    rows = nrow(data),
    cols = ncol(data)
  )

vroom::vroom_write(
  tm_df,
  here::here("inst", "bench", "all_numeric-times.tsv"),
  delim = "\t")

vroom::vroom_write(
  sessioninfo::package_info(c("vroom", "readr", "dplyr", "data.table", "base"), dependencies = FALSE, include_base = TRUE),
  here::here("inst", "bench", "sessioninfo.tsv"),
  delim = "\t"
)

file_delete(file)
