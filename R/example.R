#' Get path to vroom examples
#'
#' vroom comes bundled with a number of sample files in
#' its 'inst/extdata' directory. Use `vroom_examples()` to list all the
#' available examples and `vroom_example()` to retrieve the path to one
#' example.
#' @param path Name of file.
#' @param pattern A regular expression of filenames to match. If `NULL`, all
#'   available files are returned.
#' @export
#' @examples
#' # List all available examples
#' vroom_examples()
#'
#' # Get path to one example
#' vroom_example("mtcars.csv")
vroom_example <- function(path) {
  system.file("extdata", path, package = "vroom", mustWork = TRUE)
}

#' @rdname vroom_example
#' @export
vroom_examples <- function(pattern = NULL) {
  list.files(system.file("extdata", package = "vroom"), pattern = pattern)
}
