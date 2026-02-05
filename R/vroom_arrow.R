#' Read a delimited file into an Arrow Table
#'
#' Uses the libvroom SIMD parser to read a CSV file and return the result
#' as an Arrow Table via zero-copy Arrow C Data Interface export. This avoids
#' R's global string pool entirely, making it particularly efficient for
#' string-heavy files.
#'
#' @param file Path to a delimited file.
#' @param delim Single character used to separate fields. If `NULL`, the
#'   delimiter is auto-detected.
#' @param quote Single character used to quote strings.
#' @param col_names If `TRUE`, the first row is used as column names.
#' @param comment A string used to identify comments.
#' @param skip_empty_rows Should blank rows be ignored?
#' @param na Character vector of strings to interpret as missing values.
#' @param num_threads Number of threads to use for parsing.
#' @return An Arrow Table.
#' @export
vroom_arrow <- function(
  file,
  delim = NULL,
  quote = '"',
  col_names = TRUE,
  comment = "",
  skip_empty_rows = TRUE,
  na = c("", "NA"),
  num_threads = vroom_threads()
) {
  rlang::check_installed("arrow", reason = "to use vroom_arrow()")

  file <- standardise_path(file)
  if (length(file) != 1 || !is.character(file[[1]])) {
    cli::cli_abort("{.fn vroom_arrow} requires a single file path.")
  }

  na_str <- paste(na, collapse = ",")

  reader <- vroom_arrow_(
    path = file[[1]],
    delim = delim %||% "",
    quote = quote,
    has_header = isTRUE(col_names),
    skip = 0L,
    comment = comment,
    skip_empty_rows = skip_empty_rows,
    na_values = na_str,
    num_threads = as.integer(num_threads)
  )

  reader$read_table()
}
