#' Write a data frame to a delimited file
#'
#' @inheritParams readr::write_tsv
#' @inheritParams vroom
#' @param escape The type of escape to use when quotes are in the data.
#'   - `double` - quotes are escaped by doubling them.
#'   - `backslash` - quotes are escaped by a preceding backslash.
#'   - `none` - quotes are not escaped.
#' @param quote How to handle fields which contain characters that need to be quoted.
#'   - `needed` - Only quote fields which need them.
#'   - `all` - Quote all fields.
#'   - `none` - Never quote fields.
#' @param bom If `TRUE` add a UTF-8 BOM at the beginning of the file. This is
#'   recommended when saving data for consumption by excel, as it will force
#'   excel to read the data with the correct encoding (UTF-8)
#' @param delim Delimiter used to separate values. Defaults to `\t` to write
#'   tab separated value (TSV) files.
#' @param na String used for missing values. Defaults to 'NA'.
#' @param path `r lifecycle::badge("deprecated")` is no longer supported, use
#'   `file` instead.
#' @export
#' @examples
#' # If you only specify a file name, vroom_write() will write
#' # the file to your current working directory.
#' out_file <- tempfile(fileext = "csv")
#' vroom_write(mtcars, out_file, ",")
#'
#' # You can also use a literal filename
#' # vroom_write(mtcars, "mtcars.tsv")
#'
#' # If you add an extension to the file name, write_()* will
#' # automatically compress the output.
#' # vroom_write(mtcars, "mtcars.tsv.gz")
#' # vroom_write(mtcars, "mtcars.tsv.bz2")
#' # vroom_write(mtcars, "mtcars.tsv.xz")
vroom_write <- function(x, file, delim = '\t', eol = "\n", na = "NA", col_names = !append,
  append = FALSE, quote = c("needed", "all", "none"), escape =
    c("double", "backslash", "none"), bom = FALSE, num_threads =
    vroom_threads(), progress = vroom_progress(), path = deprecated()) {

  if (lifecycle::is_present(path)) {
    file <- path
    lifecycle::deprecate_soft(
      when = "1.5.0",
      what = "vroom_write(file)"
    )
  }


  quote <- match.arg(quote)
  escape <- match.arg(escape)

  opts <- get_vroom_write_opts(quote, escape, bom)

  # Standardise path returns a list, but we will only ever have 1 output file.
  file <- standardise_one_path(file, write = TRUE)

  # If there are no columns in the data frame, just create an empty file and return
  if (NCOL(x) == 0) {
    if (!inherits(file, "connection")) {
      file.create(file)
    }
    return(invisible(x))
  }

  # We need to convert any altrep vectors to normal vectors otherwise we can't fill the
  # write buffers from other threads.
  xx <- vroom_convert(x)
  xx[] <- lapply(xx, output_column)

  # This seems to work ok in practice
  buf_lines <- max(as.integer(Sys.getenv("VROOM_WRITE_BUFFER_LINES", nrow(x) / 100 / num_threads)), 1)

  if (inherits(file, "connection")) {
    vroom_write_connection_(xx, file, delim, eol, na_str = na, col_names = col_names,
      options = opts, num_threads = num_threads, progress = progress, buf_lines = buf_lines,
      is_stdout = file == stdout(), append = append)
  } else {
    vroom_write_(xx, file, delim, eol, na_str = na, col_names = col_names,
      append = append, options = opts,
      num_threads = num_threads, progress = progress, buf_lines = buf_lines)
  }

  invisible(x)
}


get_vroom_write_opts <- function(quote, escape, bom) {
  v_opts <- vroom_write_opts()
  bitwOr(
    v_opts[paste0("quote_", quote)],
    bitwOr(
      v_opts[paste0("escape_", escape)],
      if (bom) v_opts["bom"] else 0)
  )
}

vroom_write_opts <- function() c(
  "quote_none" = 0L,
  "escape_none" = 0L,
  "quote_needed" = 1L,
  "quote_all" = 2L,
  "escape_double" = 4L,
  "escape_backslash" = 8L,
  "bom" = 16L
)

#' Convert a data frame to a delimited string
#'
#' This is equivalent to [vroom_write()], but instead of writing to
#' disk, it returns a string. It is primarily useful for examples and for
#' testing.
#'
#' @inheritParams vroom_write
#' @export
vroom_format <- function(x, delim = "\t", eol = "\n", na = "NA", col_names = TRUE,
                         escape = c("double", "backslash", "none"),
                         quote = c("needed", "all", "none"),
                         bom = FALSE) {

  quote <- match.arg(quote)
  escape <- match.arg(escape)

  opts <- get_vroom_write_opts(quote, escape, bom)

  x[] <- lapply(x, output_column)
  vroom_format_(x, delim = delim, eol = eol, na_str = na, col_names = col_names,
                options = opts)
}

#' Write lines to a file
#'
#' @inheritParams vroom_write
#' @export
vroom_write_lines <- function(x, file, eol = "\n", na = "NA", append = FALSE, num_threads = vroom_threads()) {
  stopifnot(is.character(x))

  x <- list(X1 = x)
  class(x) <- "data.frame"
  attr(x, "row.names") <- c(NA_integer_, -length(x[[1]]))

  vroom_write(x, file = file, delim = "", col_names = FALSE, eol = eol, na =
    na, append = append, quote = "none", escape = "none", num_threads =
    num_threads
  )
}

#' Preprocess column for output
#'
#' This is a generic function that applied to each column before it is saved
#' to disk. It provides a hook for S3 classes that need special handling.
#'
#' @keywords internal
#' @param x A vector
#' @examples
#' # Most types are returned unchanged
#' output_column(1)
#' output_column("x")
#'
#' # datetimes are formatted in ISO 8601
#' output_column(Sys.Date())
#' output_column(Sys.time())
#' @export
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
  x
}

#' @export
output_column.factor <- function(x) {
  # TODO: look into doing writing directly in C++
  as.character(x)
}
