#' Retrieve parsing problems
#'
#' vroom will only fail to parse a file if the file is invalid in a way that is
#' unrecoverable. However there are a number of non-fatal problems that you
#' might want to know about. You can retrieve a data frame of these problems
#' with this function.
#'
#' @param x A data frame from `vroom::vroom()`.
#' @param lazy If `TRUE`, just the problems found so far are returned. If
#'   `FALSE` (the default) the lazy data is first read completely and all
#'   problems are returned.
#' @return A data frame with one row for each problem and four columns:
#'   - row,col - Row and column number that caused the problem, referencing the
#'   original input
#'   - expected - What vroom expected to find
#'   - actual - What it actually found
#'   - file - The file with the problem
#' @export
problems <- function(x = .Last.value, lazy = FALSE) {
  if (!inherits(x, "tbl_df")) {
    cli::cli_abort(c(
      "The {.arg x} argument of {.fun vroom::problems} must be a data frame created by vroom:",
      x = "{.arg x} has class {.cls {class(x)}}"
    ))
  }

  if (!isTRUE(lazy)) {
    vroom_materialize(x, replace = FALSE)
  }

  probs <- attr(x, "problems")
  if (typeof(probs) != "externalptr") {
    cli::cli_abort(c(
      "The {.arg x} argument of {.fun vroom::problems} must be a data frame created by vroom:",
      x = "{.arg x} seems to have been created with something else, maybe readr?"
    ))
  }
  probs <- vroom_errors_(probs)
  probs <- probs[!duplicated(probs), ]
  probs <- probs[order(probs$file, probs$row, probs$col), ]

  tibble::as_tibble(probs)
}
