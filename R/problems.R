#' keywords internal
#' @export
problems <- function(x) {
  probs <- vroom_errors_(attr(x, "errors"))
  probs[order(probs$file, probs$row, probs$col), ]
}
