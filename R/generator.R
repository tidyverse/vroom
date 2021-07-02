#' Generate individual vectors of the types supported by vroom
#'
#' @param n The size of the vector to generate
#' @param min The minimum range for the vector
#' @param max The maximum range for the vector
#' @param values The explicit values to use.
#' @param f The random function to use.
#' @inheritParams base::sample.int
#' @param ... Additional arguments passed to internal generation functions
#' @name generators
#' @examples
#' # characters
#' gen_character(4)
#'
#' # factors
#' gen_factor(4)
#'
#' # logical
#' gen_logical(4)
#'
#' # numbers
#' gen_double(4)
#' gen_integer(4)
#'
#' # temporal data
#' gen_time(4)
#' gen_date(4)
#' gen_datetime(4)
#' @export
gen_character <- function(n, min = 5, max = 25, values = c(letters, LETTERS, 0:9), ...) {

  if (min > max) {
    max <- min
  }

  # The seed for the C++ RNG used is an unsigned 32 bit integer, which is why I
  # multiply int max by 2. Possibly an off by one error here though...
  seeds <- sample.int(2 * .Machine$integer.max, 2)

  gen_character_(n, min, max, paste(values, collapse = ""), seeds[[1]], seeds[[2]])
}

#' @rdname generators
#' @export
gen_double <- function(n, f = stats::rnorm, ...) {
  f(n, ...)
}

#' @rdname generators
#' @export
gen_number <- gen_double

#' @rdname generators
#' @export
gen_integer <- function(n, min = 1L, max = .Machine$integer.max, prob = NULL, ...) {
  max <- max - min + 1L
  sample.int(max, size = n, replace = TRUE, prob = prob) + min - 1L
}

#' @rdname generators
#' @param num_levels The number of factor levels to generate
#' @param ordered Should the factors be ordered factors?
#' @param levels The explicit levels to use, if `NULL` random levels are generated using [gen_name()].
#' @export
gen_factor <- function(n, levels = NULL, ordered = FALSE, num_levels = gen_integer(1L, 1L, 25L), ...) {
  if (is.null(levels)) {
    levels <- gen_name(num_levels)
  }

  res <- gen_integer(n, max = length(levels), ...)

  attr(res, "levels") <- levels
  if (ordered) {
    class(res) <- c("ordered", "factor")
  } else {
    class(res) <- "factor"
  }
  res
}

#' @rdname generators
#' @param fractional Whether to generate times with fractional seconds
#' @export
gen_time <- function(n, min = 0, max = hms::hms(days = 1), fractional = FALSE, ...) {
  res <- hms::hms(seconds = stats::runif(n, min = min, max = max))
  if (!fractional) {
    res <- hms::as_hms(floor(res))
  }
  res
}

#' @rdname generators
#' @export
gen_date <- function(n, min = as.Date("2001-01-01"), max = as.Date("2021-01-01"), ...) {
  structure(as.numeric(gen_integer(n, min = min, max = max)), class = "Date")
}

#' @rdname generators
#' @param tz The timezone to use for dates
#' @export
gen_datetime <- function(n, min = as.POSIXct("2001-01-01"), max = as.POSIXct("2021-01-01"), tz = "UTC", ...) {
  structure(stats::runif(n, min = min, max = max), class = c("POSIXct", "POSIXt"), tzone = tz)
}

#' @rdname generators
#' @export
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
#'
#' There is also a family of functions to generate individual vectors of each
#' type.
#'
#' @param rows Number of rows to generate
#' @param cols Number of columns to generate, if `NULL` this is derived from `col_types`.
#' @param missing The percentage (from 0 to 1) of missing data to use
#' @seealso [generators] to generate individual vectors.
#' @inheritParams vroom
#' @examples
#' # random 10 x 5 table with random column types
#' rand_tbl <- gen_tbl(10, 5)
#' rand_tbl
#'
#' # all double 25 x 4 table
#' dbl_tbl <- gen_tbl(25, 4, col_types = "dddd")
#' dbl_tbl
#'
#' # Use the dots in long form column types to change the random function and options
#' types <- rep(times = 4, list(col_double(f = stats::runif, min = -10, max = 25)))
#' types
#' dbl_tbl2 <- gen_tbl(25, 4, col_types = types)
#' dbl_tbl2
#' @export
gen_tbl <- function(rows, cols = NULL, col_types = NULL, locale = default_locale(), missing = 0) {

  if (is.null(cols) && is.null(col_types)) {
    stop("One of `cols` or `col_types` must be set", call. = FALSE)
  }

  spec <- as.col_spec(col_types)

  if (is.null(cols)) {
    cols <- length(spec$cols)
  }

  nms <- make_names(names(spec$cols), cols)

  specs <- col_types_standardise(spec, length(nms), nms, vroom_enquo(rlang::quo(NULL)), "unique")
  res <- vector("list", cols)
  for (i in seq_len(cols)) {
    type <- sub("collector_", "", class(specs$cols[[i]])[[1]])
    if (type == "guess") {
      type <- sample(all_col_types[["type"]], 1)
      specs$cols[[i]] <- do.call(paste0("col_", type), list())
    }
    fun_nme <- paste0("gen_", type)
    res[[i]] <- do.call(fun_nme, c(rows, specs$cols[[i]]))
  }

  if (missing > 0) {
    res[] <- lapply(res, function(x) {
      x[sample(c(TRUE, FALSE), size = rows, prob = c(missing, 1 - missing), replace = TRUE)] <- NA
      x
    })
  }
  names(res) <- nms
  attr(res, "spec") <- specs
  tibble::as_tibble(res)
}

# Name and adjective list from https://github.com/rstudio/cranwhales/blob/93349fe1bc790f115a3d56660b6b99ffe258d9a2/random-names.R
#' @rdname generators
#' @export
gen_name <- local({

  # This will run during build / installation, but that is OK
  adjectives <- readLines(system.file("words", "adjectives.txt", package = "vroom"))
  animals <- readLines(system.file("words", "animals.txt", package = "vroom"))

  function(n) {
    paste0(sample(adjectives, n, replace = TRUE), "_", sample(animals, n, replace = TRUE))
  }
})
