#' @export
vroom_write <- function(df, out, delim = '\t', col_names = TRUE, lines = 100, num_threads = vroom_threads()) {
  df_in <- df
  df[] <- lapply(df, readr::output_column)
  vroom_write_(df, out, delim, col_names = col_names, lines, num_threads)

  invisible(df_in)
}

#' @export
vroom_format <- function(df, col_names = TRUE) {
  vroom_format_(df, col_names = col_names)
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
