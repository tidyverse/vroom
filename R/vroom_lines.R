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
vroom_lines <- function(
  file,
  n_max = Inf,
  skip = 0,
  na = character(),
  skip_empty_rows = FALSE,
  locale = default_locale(),
  altrep = TRUE,
  num_threads = vroom_threads(),
  progress = vroom_progress()
) {
  file <- standardise_path(file)

  if (!is_ascii_compatible(locale$encoding)) {
    file <- reencode_file(file, locale$encoding)
    locale$encoding <- "UTF-8"
  }

  if (length(file) == 0 || identical(n_max, 0) || identical(n_max, 0L)) {
    return(character())
  }

  has_limit <- is.finite(n_max) && n_max >= 0
  rows_remaining <- if (has_limit) as.integer(n_max) else -1L
  na_str <- paste(na, collapse = ",")

  results <- list()

  for (input in file) {
    if (has_limit && rows_remaining == 0L) {
      break
    }

    # Handle compressed/remote files via connections
    if (is.character(input) && (is_url(input) || is_compressed_path(input))) {
      input <- connection_or_filepath(input)
    }
    # Non-ASCII paths need R connection for proper encoding handling
    if (is.character(input) && !is_ascii_path(input)) {
      input <- file(input)
    }

    if (inherits(input, "connection")) {
      input <- read_connection_raw(input)
      if (length(input) == 0L) {
        next
      }
    }

    # Handle empty files before calling C++
    if (is.character(input) && file.exists(input) && file.size(input) == 0L) {
      next
    }

    res <- vroom_lines_libvroom_(
      input = input,
      skip = as.integer(skip),
      n_max = rows_remaining,
      na_values = na_str,
      skip_empty_rows = skip_empty_rows,
      num_threads = as.integer(num_threads),
      use_altrep = isTRUE(altrep)
    )

    if (length(res) > 0) {
      results[[length(results) + 1L]] <- res
      if (has_limit) {
        rows_remaining <- rows_remaining - length(res)
      }
    }
  }

  if (length(results) == 0) {
    return(character())
  }

  if (length(results) == 1) results[[1]] else unlist(results)
}
