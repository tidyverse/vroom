#' Get path to readr example
#'
#' vroom comes bundled with a number of sample files in
#' its 'inst/extdata' directory. This function make them
#' easy to access
#' @param path Name of file. If 'NULL', all available example files will be
#' listed.
#' @export
#' @examples
#' vroom_example()
#' vroom_example("mtcars.csv")
vroom_example <- function (path = NULL) {
  if (is.null(path)) {
    dir(system.file("extdata", package = "vroom"))
  }
  else {
    system.file("extdata", path, package = "vroom", mustWork = TRUE)
  }
}
