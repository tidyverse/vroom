#' Structure of objects
#'
#' Similar to `str()` but with more information for Altrep objects.
#'
#' @param x a vector
#' @examples
#' # when used on non-altrep objects altrep will always be false
#' vroom_str(mtcars)
#'
#' mt <- vroom(vroom_example("mtcars.csv"), ",", altrep = c("chr", "dbl"))
#' vroom_str(mt)
#' @export
vroom_str <- function(x) {
  UseMethod("vroom_str")
}

#' @export
vroom_str.data.frame <- function(x) {
  classes <- glue::glue_collapse(glue::single_quote(class(x)), ", ", last = ", and ")
  rows <- nrow(x)
  cols <- ncol(x)

  cat(glue::glue("{classes}: {rows} obs., {cols} vars.:\n\n"), sep = "")
  nms <- names(x)
  for (i in seq_along(x)) {
    cat("$", nms[[i]], ":\t", sep = "")
    vroom_str(x[[i]])
  }
}

#' @export
vroom_str.default <- function(x) {
  cat(vroom_str_(x))
}
