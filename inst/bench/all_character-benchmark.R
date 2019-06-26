args <- commandArgs(trailingOnly = TRUE)
str(args)
rows <- as.integer(args[[1]])
cols <- as.integer(args[[2]])

set.seed(42)
RNGversion("3.5.3")

library(fs)

file <- path(tempdir(), "all-character.tsv")

# Generate one factor column that can be used for filtering and the rest random
# length characters
library(vroom)

# We want ~ 1000 rows to filter
num_levels <- 5
levels <- c("helpless_sheep", gen_name(num_levels - 1))

filt_p <- 1000 / rows

# The prob for the rest should just be evenly spaced
rest_p <- rep((1 - filt_p) / (num_levels - 1), num_levels - 1)

col_types <-  stats::setNames(
  c(list(
      col_factor(levels = levels, prob = c(filt_p, rest_p))),
    rep(list(col_character()), cols - 1)
  ), make.names(seq_len(cols)))

data <- gen_tbl(rows, cols, col_types = col_types)

vroom_write(data, file, "\t")

desc <- c("setup", "read", "print", "head", "tail", "sample", "filter", "aggregate")

vroom_base <- function(file, desc) {
  bench::workout(description = desc,
    {
      library(vroom)
      x <- vroom(file, trim_ws = FALSE, quote = "", escape_double = FALSE, na = character())
      print(x)
      a <- head(x)
      b <- tail(x)
      c <- x[sample(NROW(x), 100), ]
      d <- x[x$X1 == "helpless_sheep", ]
      e <- tapply(x$X2, x$X1, function(x) mean(nchar(x)))
    }
  )
}

`vroom (full altrep)_base` <- function(file, desc) {
  bench::workout(description = desc,
    {
    ({library(vroom)})
      x <- vroom(file, trim_ws = FALSE, quote = "", escape_double = FALSE, na = character(), altrep_opts = TRUE)
      print(x)
      a <- head(x)
      b <- tail(x)
      c <- x[sample(NROW(x), 100), ]
      d <- x[x$X1 == "helpless_sheep", ]
      e <- tapply(x$X2, x$X1, function(x) mean(nchar(x)))
    }
  )
}

`vroom (no altrep)_base` <- function(file, desc) {
  bench::workout(description = desc,
    {
    ({library(vroom)})
      x <- vroom(file, trim_ws = FALSE, quote = "", escape_double = FALSE, na = character(), altrep_opts = FALSE)
      print(x)
      a <- head(x)
      b <- tail(x)
      c <- x[sample(NROW(x), 100), ]
      d <- x[x$X1 == "helpless_sheep", ]
      e <- tapply(x$X2, x$X1, function(x) mean(nchar(x)))
    }
  )
}

vroom_dplyr <- function(file, desc) {
  bench::workout(description = desc,
    {
      ({ library(vroom); library(dplyr) })
      x <- vroom(file, trim_ws = FALSE, quote = "", escape_double = FALSE, na = character())
      print(x)
      a <- head(x)
      b <- tail(x)
      c <- sample_n(x, 100)
      d <- filter(x, X1 == "helpless_sheep")
      e <- group_by(x, X1) %>% summarise(avg_nchar = mean(nchar(X2)))
    }
  )
}

`vroom (full altrep)_dplyr` <- function(file, desc) {
  bench::workout(description = desc,
    {
      ({library(vroom); library(dplyr)})
      x <- vroom(file, trim_ws = FALSE, quote = "", escape_double = FALSE, na = character(), altrep_opts = TRUE)
      print(x)
      a <- head(x)
      b <- tail(x)
      c <- sample_n(x, 100)
      d <- filter(x, X1 == "helpless_sheep")
      e <- group_by(x, X1) %>% summarise(avg_nchar = mean(nchar(X2)))
    }
  )
}

`vroom (no altrep)_dplyr` <- function(file, desc) {
  bench::workout(description = desc,
    {
      ({library(vroom); library(dplyr)})
      x <- vroom(file, trim_ws = FALSE, quote = "", escape_double = FALSE, na = character(), altrep_opts = FALSE)
      print(x)
      a <- head(x)
      b <- tail(x)
      c <- sample_n(x, 100)
      d <- filter(x, X1 == "helpless_sheep")
      e <- group_by(x, X1) %>% summarise(avg_nchar = mean(nchar(X2)))
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
      d <- x[X1 == "helpless_sheep", ]
      e <- x[ , .(mean(nchar(X2))), by = X1]
    }
  )
}

readr <- function(file, desc) {
  bench::workout(description = desc,
    {
    ({ library(readr); library(dplyr) })
      x <- read_tsv(file, trim_ws = FALSE, quote = "", na = character())
      print(x)
      a <- head(x)
      b <- tail(x)
      c <- sample_n(x, 100)
      d <- filter(x, X1 == "helpless_sheep")
      e <- group_by(x, X1) %>% summarise(avg_nchar = mean(nchar(X2)))
    }
  )
}

read.delim <- function(file, desc) {
  bench::workout(description = desc,
    {
      ({})
      x <- read.delim(file, quote = "", na.strings = NULL, stringsAsFactors = FALSE)
      print(head(x, 10))
      a <- head(x)
      b <- tail(x)
      c <- x[sample(NROW(x), 100), ]
      d <- x[x$X1 == "helpless_sheep", ]
      e <- tapply(x$X2, x$X1, function(x) mean(nchar(x)))
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
  mutate(
    size = file.size(file),
    rows = nrow(data),
    cols = ncol(data)
  )

vroom::vroom_write(
  tm_df,
  here::here("inst", "bench", "all_character-times.tsv"),
  delim = "\t")

vroom::vroom_write(
  sessioninfo::package_info(c("vroom", "readr", "dplyr", "data.table", "base"), dependencies = FALSE, include_base = TRUE),
  here::here("inst", "bench", "sessioninfo.tsv"),
  delim = "\t"
)

file_delete(file)
