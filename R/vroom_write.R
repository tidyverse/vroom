#' Write a data frame to a delimited file
#'
#' @inheritParams readr::write_tsv
#' @export
vroom_write <- function(x, out, delim = '\t', na = "NA", col_names = !append, append = FALSE, num_threads = vroom_threads(), progress = vroom_progress()) {
  # Standardise path returns a list, but we will only ever have 1 output file.
  out <- standardise_one_path(out, check = FALSE)

  x_in <- x
  x[] <- lapply(x, output_column)

  if (inherits(out, "connection")) {
    vroom_write_connection_(x, out, delim, na_str = na, col_names = col_names, append = append, num_threads = num_threads, progress = progress,
      buf_lines = as.numeric(Sys.getenv("VROOM_WRITE_BUFFER_SIZE", 1000)))
  } else {
    vroom_write_(x, out, delim, na_str = na, col_names = col_names, append = append, num_threads = num_threads, progress = progress,
      buf_lines = as.numeric(Sys.getenv("VROOM_WRITE_BUFFER_SIZE", 1000)))
  }

  invisible(x_in)
}

#' @export
vroom_format <- function(x, delim = '\t', na = "NA", col_names = TRUE) {
  x[] <- lapply(x, output_column)
  vroom_format_(x, delim = delim, na_str = na, col_names = col_names)
}

#' Preprocess column for output
#'
#' This is a generic function that applied to each column before it is saved
#' to disk. It provides a hook for S3 classes that need special handling.
#'
#' @keywords internal
#' @param x A vector
#' @export
#' @examples
#' # Most columns are left as is, but POSIXct are
#' # converted to ISO8601.
#' x <- parse_datetime("2016-01-01")
#' str(output_column(x))
output_column <- function(x) {
  UseMethod("output_column")
}

#' @export
output_column.default <- function(x) {
  if (!is.object(x)) return(x)
  as.character(x)
}

#' @export
output_column.double <- function(x) {
  x
}

#' @export
output_column.POSIXt <- function(x) {
  format(x, "%Y-%m-%dT%H:%M:%OSZ", tz = "UTC", justify = "none")
}

#' @export
output_column.character <- function(x) {
  enc2utf8(x)
}

#' @export
output_column.factor <- function(x) {
  # TODO: look into doing writing directly in C++
  as.character(x)
}
