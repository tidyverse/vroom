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
vroom_lines <- function(file, n_max = Inf, skip = 0, altrep_opts = "chr",
  num_threads = vroom_threads(), progress = vroom_progress()) {

  file <- standardise_path(file)

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
  out <- vroom_(file, delim = "\1", col_names = "V1", col_types = "c",
    id = NULL, skip = skip, col_select = col_select, na = character(), quote = "",
    trim_ws = FALSE, escape_double = FALSE, escape_backslash = FALSE, comment = "",
    locale = default_locale(),
    guess_max = 0, n_max = n_max, altrep_opts = vroom_altrep_opts(altrep_opts),
    num_threads = num_threads, progress = progress
  )

  out[[1]]
}
