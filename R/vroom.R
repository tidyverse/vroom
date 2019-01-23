#' @useDynLib vroom, .registration = TRUE
#' @importFrom Rcpp sourceCpp
NULL

#' Read a delimited file into a tibble
#'
#' @inheritParams readr::read_delim
#' @param file path to a local file.
#' @param num_threads Number of threads to use when reading and materializing vectors.
#' @export
#' @examples
#' \dontshow{
#' .old_wd <- setwd(tempdir())
#' }
#'
#' readr::write_tsv(mtcars, "mtcars.tsv")
#' vroom("mtcars.tsv")
#'
#' \dontshow{
#' unlink("mtcars.tsv")
#' setwd(.old_wd)
#' }
vroom <- function(file, delim = "\t", col_names = TRUE, skip = 0, na = c("", "NA"), quote = '"', num_threads = parallel::detectCores()) {

  file <- standardise_path(file)

  out <- vroom_(file, delim = delim, col_names = col_names, skip = skip, na = na, quote = quote, num_threads = num_threads)

  if (is.null(names(out))) {
    names(out) <- make.names(seq_along(out))
  }

  tibble::as_tibble(out)
}

#' Guess the type of a vector
#'
#' @inheritParams readr::guess_parser
#' @export
guess_type <- function(x, na = c("", "NA"), locale = readr::default_locale(), guess_integer = FALSE) {

  x[x %in% na] <- NA

  type <- readr::guess_parser(x, locale = locale, guess_integer = guess_integer)
  switch(type,
    "double" = 1L,
    "integer" = 2L,
    "logical" = 3L,
    0L
  )
}
