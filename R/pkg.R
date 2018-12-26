#' @useDynLib readidx, .registration = TRUE
#' @importFrom Rcpp sourceCpp
NULL

#' @export
read_tsv <- function(x) {
  out <- read_tsv_(x, skip = 1)

  tibble::as_tibble(out)
}
