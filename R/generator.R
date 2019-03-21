gen_character <- function(n, min = 5, max = 25, values = c(letters, LETTERS, 0:9), ...) {
  #replicate(n, paste(sample(values, sample.int(max - min) + min, replace = TRUE), collapse = ""), simplify = "vector")

  if (min > max) {
    max <- min
  }

  gen_character_(n, min, max, paste(values, collapse = ""))
}

gen_double <- function(n, f = stats::rnorm, ...) {
  f(n)
}

gen_number <- gen_double

gen_integer <- function(n, min = 1L, max = .Machine$integer.max, ...) {
  max <- max - min + 1L
  sample.int(max, size = n, replace = TRUE) + min - 1L
}

gen_factor <- function(n, levels = NULL, ordered = FALSE, include_na = FALSE, num_levels = gen_integer(1L, 1L, 25L), ...) {
  if (is.null(levels)) {
    levels <- gen_character(num_levels)
  }

  res <- gen_integer(n, max = length(levels))

  attr(res, "levels") <- levels
  if (ordered) {
    class(res) <- c("ordered", "factor")
  } else {
    class(res) <- "factor"
  }
  res
}

gen_time <- function(n, min = 0, max = hms::hms(days = 1), fractional = FALSE, ...) {
  res <- hms::hms(seconds = runif(n, min = min, max = max))
  if (!fractional) {
    res <- hms::as.hms(floor(res))
  }
  res
}

gen_date <- function(n, min = as.Date("2001-01-01"), max = as.Date("2021-01-01"), ...) {
  structure(as.numeric(gen_integer(n, min = min, max = max)), class = "Date")
}

gen_datetime <- function(n, min = as.POSIXct("2001-01-01"), max = as.POSIXct("2021-01-01"), tz = "UTC", ...) {
  structure(runif(n, min = min, max = max), class = c("POSIXct", "POSIXt"), tzone = tz)
}

gen_logical <- function(n, ...) {
  c(TRUE, FALSE)[sample.int(n = 2, n, replace = TRUE)]
}

all_col_types <- tibble::tribble(
  ~ type, ~ class,
  "character", "character",
  "factor", "character",
  "double", "numeric",
  "integer", "numeric",
  "number", "numeric",
  "date", "temporal",
  "datetime", "temporal",
  "time", "temporal",
)

#' Generate a random tibble
#'
#' This is useful for benchmarking, but also for bug reports when you cannot
#' share the real dataset.
#' @param rows Number of rows to generate
#' @param cols Number of columns to generate
#' @inheritParams vroom
#' @export
gen_tbl <- function(rows, cols, col_types = NULL, locale = default_locale()) {
  nms <- paste0("V", seq_len(cols))
  specs <- col_types_standardise(col_types, nms)
  res <- vector("list", cols)
  for (i in seq_len(cols)) {
    type <- sub("collector_", "", class(specs$cols[[i]])[[1]])
    if (type == "guess") {
      type <- sample(all_col_types[["type"]], 1)
      specs$cols[[i]] <- do.call(paste0("col_", type), list())
    }
    fun_nme <- paste0("gen_", type)
    res[[i]] <- do.call(fun_nme, c(rows, specs$cols[[i]], locale))
  }
  names(res) <- nms
  attr(res, "spec") <- specs
  tibble::as_tibble(res)
}

gen_write <- function(x, path, delim, na = "NA", append = FALSE, col_names =
  !append, col_types = NULL, locale = default_locale(), quote_escape = "double") {
  if (is.null(col_types) && inherits(x, "tbl_df")) {
    col_types <- readr::spec(x)
  }

  specs <- col_types_standardise(col_types, colnames(x))

  for (i in seq_along(x)) {
    if (inherits(x[[i]], "hms")) {
      if (nzchar(specs$cols[[i]]$format)) {
        x[[i]] <- do.call(as.character, c(list(as.POSIXlt(x[[i]])), specs$cols[[i]]))
      }
    }
    x[[i]] <- do.call(as.character, c(list(x[[i]]), specs$cols[[i]]))
  }
  readr::write_delim(x, path, delim, na = na, append = append, col_names = col_names)
}
