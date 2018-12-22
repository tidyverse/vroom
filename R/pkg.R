#' @useDynLib readidx, .registration = TRUE
#' @importFrom Rcpp sourceCpp
NULL

#' @export
read_tsv <- function(x) {
  out <- create_index_(x)
  names(out) <- letters[seq_len(length(out))]

  tibble::as_tibble(out)
}
