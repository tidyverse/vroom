#' keywords internal
#' @export
problems <- function(x) {
  probs <- vroom_errors_(attr(x, "errors"))
  probs <- probs[order(probs$file, probs$row, probs$col), ]

  tibble::as_tibble(probs)
}
