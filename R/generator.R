gen_character <- function(n, min = 5, max = 25, values = c(letters, LETTERS, 0:9)) {
  gen_character_(n, min, max, paste(values, collapse = ""))
  #replicate(n, paste(sample(values, sample.int(max - min) + min, replace = TRUE), collapse = ""), simplify = "vector")
}

gen_double <- function(n, f = rnorm, ...) {
  f(n, ...)
}

gen_number <- gen_double

gen_integer <- function(n, min = 1L, max = .Machine$integer.max, ...) {
  max <- max - min + 1L
  sample.int(max, size = n, replace = TRUE, ...) + min - 1L
}

gen_factor <- function(n, levels = NULL, ordered = FALSE, include_na = FALSE, num_levels = gen_integer(1L, 1L, 25L)) {
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

all_col_types <- tibble::tribble(
  ~ type, ~ class,
  "character", "character",
  "factor", "character",
  "double", "numeric",
  "integer", "numeric",
  "number", "numeric",
  #"date", "temporal",
  #"datetime", "temporal",
  #"time", "temporal",
)

#' Generate a random tibble
#'
#' This is useful for benchmarking, but also for bug reports when you cannot
#' share the real dataset.
#' @param rows Number of rows to generate
#' @param cols Number of columns to generate
#' @inheritParams vroom
gen_tbl <- function(rows, cols, col_types = NULL) {
  nms <- paste0("V", seq_len(cols))
  specs <- col_types_standardise(col_types, nms)
  res <- vector("list", cols)
  for (i in seq_len(cols)) {
    type <- sub("collector_", "", class(specs$cols[[i]])[[1]])
    if (type == "guess") {
      type <- sample(all_col_types, 1)
      specs$cols[[i]] <- do.call(paste0("col_", type), list())
    }
    fun_nme <- paste0("gen_", type)
    res[[i]] <- do.call(fun_nme, c(rows, specs$cols[[i]]))
  }
  names(res) <- nms
  attr(res, "spec") <- specs
  tibble::as_tibble(res)
}
