#' @useDynLib readidx, .registration = TRUE
#' @importFrom Rcpp sourceCpp
NULL

#' @export
read_tsv <- function(x) {
  out <- read_tsv_(x, skip = 1)

  tibble::as_tibble(out)
}

#' @export
guess_type <- function(x) {
  type <- readr::guess_parser(x[seq_len(min(length(x), 100))])
  switch(type,
    "double" = 1L,
    0L
  )
}
