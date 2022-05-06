#' Read lines from a file
#'
#' `vroom_lines()` is similar to `readLines()`, however it reads the lines
#' lazily like [vroom()], so operations like `length()`, `head()`, `tail()` and `sample()`
#' can be done much more efficiently without reading all the data into R.
#' @inheritParams vroom
#' @examples
#' lines <- vroom_lines(vroom_example("mtcars.csv"))
#'
#' length(lines)
#' head(lines, n = 2)
#' tail(lines, n = 2)
#' sample(lines, size = 2)
#' @export
vroom_lines <- function(file, n_max = Inf, skip = 0,
  na = character(), skip_empty_rows = FALSE,
  locale = default_locale(), altrep = TRUE,
  altrep_opts = deprecated(), num_threads = vroom_threads(),
  progress = vroom_progress()) {

  if (!rlang::is_missing(altrep_opts)) {
    lifecycle::deprecate_warn("1.1.0", "vroom_lines(altrep_opts = )", "vroom_lines(altrep = )")
    altrep <- altrep_opts
  }

  file <- standardise_path(file)

  if (!is_ascii_compatible(locale$encoding)) {
    file <- reencode_file(file, locale$encoding)
    locale$encoding <- "UTF-8"
  }

  if (n_max < 0 || is.infinite(n_max)) {
    n_max <- -1
  }

  if (length(file) == 0 || n_max == 0) {
    return(character())
  }

  col_select <- rlang::quo(NULL)

  # delim = "\1" sets the delimiter to be start of header, which should never
  # appear in modern text. This essentially means the only record breaks will
  # be newlines. Ideally this would be "\0", but R doesn't let you have nulls
  # in character vectors.
  out <- vroom_(file, delim = "\1", col_names = "V1", col_types = cols(col_character()),
    id = NULL, skip = skip, col_select = col_select, name_repair = "minimal",
    na = na, quote = "", trim_ws = FALSE, escape_double = FALSE,
    escape_backslash = FALSE, comment = "", skip_empty_rows = skip_empty_rows,
    locale = locale, guess_max = 0, n_max = n_max,
    altrep = vroom_altrep(altrep), num_threads = num_threads,
    progress = progress
  )
  if (length(out) == 0) {
    return(character())
  }

  out[[1]]
}
