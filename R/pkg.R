#' @useDynLib vroom, .registration = TRUE
#' @importFrom Rcpp sourceCpp
NULL

#' Read a tab delimited file into a tibbe
#'
#' @inheritParams readr::read_tsv
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
vroom <- function(file, num_threads = parallel::detectCores()) {
  out <- read_tsv_(path.expand(file), skip = 1, num_threads = num_threads)

  tibble::as_tibble(out)
}

#' Guess the type of a vector
#'
#' @inheritParams readr::guess_parser
#' @export
guess_type <- function(x, locale = readr::default_locale(), guess_integer = FALSE) {
  type <- readr::guess_parser(x, locale = locale, guess_integer = guess_integer)
  switch(type,
    "double" = 1L,
    "integer" = 2L,
    "logical" = 3L,
    0L
  )
}
