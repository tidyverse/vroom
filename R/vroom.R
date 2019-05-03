#' @useDynLib vroom, .registration = TRUE
#' @importFrom Rcpp sourceCpp
NULL

#' Read a delimited file into a tibble
#'
#' @inheritParams readr::read_delim
#' @param file path to a local file.
#' @param delim One of more characters used to delimiter fields within a
#'   record. If `NULL` the delimiter is guessed from the set of `c(",", "\\t", " ",
#'   "|", ":", ";", "\\n")`.
#' @param num_threads Number of threads to use when reading and materializing vectors.
#' @param escape_double Does the file escape quotes by doubling them?
#'   i.e. If this option is `TRUE`, the value '""' represents
#'   a single quote, '"'.
#' @param id Either a string or 'NULL'. If a string, the output will contain a
#'   variable with that name with the filename(s) as the value. If 'NULL', the
#'   default, no variable will be created.
#' @param col_select One or more selection expressions, like in
#'   `dplyr::select()`. Use `c()` or `list()` to use more than one expression.
#'   See `?dplyr::select` for details on available selection options.
#' @param .name_repair Handling of column names. By default, vroom ensures
#'   column names are not empty and unique. See `.name_repair` as documented in
#'   [tibble::tibble()] for additional options including supplying user defined
#'   name repair functions.
#' @param altrep_opts Control which column types use Altrep representations,
#'   either a character vector of types, `TRUE` or `FALSE`. See
#'   [vroom_altrep_opts()] for for full details.
#' @export
#' @examples
#' \dontshow{
#' .old_wd <- setwd(tempdir())
#' mt <- vroom(vroom_example("mtcars.csv"))
#' vroom_write(mt, "mtcars.tsv")
#' vroom_write(mt, "mtcars.tsv.gz")
#' vroom_write(mt, "mtcars.tsv.bz2")
#' }
#'
#' # Input sources -------------------------------------------------------------
#' # Read from a path
#' vroom("mtcars.tsv")
#' vroom("mtcars.tsv.gz")
#' vroom("mtcars.tsv.bz2")
#'
#' \dontrun{
#' # Including remote paths
#' vroom("https://github.com/r-lib/vroom/raw/master/inst/extdata/mtcars.csv")
#' }
#'
#' # Or directly from a string (must contain a trailing newline)
#' vroom("x,y\n1,2\n3,4\n")
#'
#' # Column selection ----------------------------------------------------------
#' # Pass column names or indexes directly to select them
#' vroom("mtcars.tsv", col_select = c(model, cyl, gear))
#' vroom("mtcars.tsv", col_select = c(1, 3, 11))
#'
#' # Or use the selection helpers
#' vroom("mtcars.tsv", col_select = starts_with("d"))
#'
#' # You can also rename specific columns
#' vroom("mtcars.tsv", col_select = list(car = model, everything()))
#'
#' # Column types --------------------------------------------------------------
#' # By default, vroom guesses the columns types, looking at 1000 rows
#' # throughout the dataset.
#' # You can specify them explcitly with a compact specification:
#' vroom("x,y\n1,2\n3,4\n", col_types = "dc")
#'
#' # Or with a list of column types:
#' vroom("x,y\n1,2\n3,4\n", col_types = list(col_double(), col_character()))
#'
#' # File types ----------------------------------------------------------------
#' # csv
#' vroom("a,b\n1.0,2.0\n", delim = ",")
#' # tsv
#' vroom("a\tb\n1.0\t2.0\n")
#' # Other delimiters
#' vroom("a|b\n1.0|2.0\n", delim = "|")
#' \dontshow{
#' unlink(c("mtcars.tsv", "mtcars.tsv.gz", "mtcars.tsv.bz2"))
#' setwd(.old_wd)
#' }
vroom <- function(file, delim = NULL, col_names = TRUE, col_types = NULL,
  col_select = NULL,
  id = NULL, skip = 0, n_max = Inf,
  na = c("", "NA"), quote = '"', comment = "", trim_ws = TRUE,
  escape_double = TRUE, escape_backslash = FALSE, locale = default_locale(),
  guess_max = 100, altrep_opts = "chr", num_threads = vroom_threads(), progress = vroom_progress(),
  .name_repair = "unique") {


  file <- standardise_path(file)

  if (length(file) == 0 || (n_max == 0 & identical(col_names, FALSE))) {
    return(tibble::tibble())
  }

  if (n_max < 0 || is.infinite(n_max)) {
    n_max <- -1
  }

  if (guess_max < 0 || is.infinite(guess_max)) {
    guess_max <- -1
  }

  # Workaround weird RStudio / Progress bug: https://github.com/r-lib/progress/issues/56#issuecomment-384232184
  if (
    isTRUE(progress) &&
    is_windows() &&
    identical(Sys.getenv("RSTUDIO"), "1")) {
    Sys.setenv("RSTUDIO" = "1")
  }

  col_select <- vroom_enquo(rlang::enquo(col_select))

  out <- vroom_(file, delim = delim, col_names = col_names, col_types = col_types,
    id = id, skip = skip, col_select = col_select,
    na = na, quote = quote, trim_ws = trim_ws, escape_double = escape_double,
    escape_backslash = escape_backslash, comment = comment, locale = locale,
    guess_max = guess_max, n_max = n_max, altrep_opts = vroom_altrep_opts(altrep_opts),
    num_threads = num_threads, progress = progress)

  out <- tibble::as_tibble(out, .name_repair = .name_repair)

  out <- vroom_select(out, col_select)

  if (is.null(col_types)) {
    show_spec_summary(out, locale = locale)
  }

  out
}

make_names <- function(x, len) {
  if (len == 0) {
    return(character())
  }

  if (length(x) == len) {
    return(x)
  }

  if (length(x) > len) {
    return(x[seq_len(len)])
  }

  nms <- make.names(seq_len(len))
  nms[seq_along(x)] <- x
  nms
}

#' Determine if progress bars should be shown
#'
#' Progress bars are shown _unless_ one of the following is `TRUE`
#' - The bar is explicitly disabled by setting `Sys.getenv("VROOM_SHOW_PROGRESS"="false")`
#' - The code is run in a non-interactive session (`interactive()` is `FALSE`).
#' - The code is run in an RStudio notebook chunk.
#' - The code is run by knitr / rmarkdown.
#' - The code is run by testthat (the `TESTTHAT` envvar is `true`).
#' @export
#' @examples
#' vroom_progress()
vroom_progress <- function() {
  env_to_logical("VROOM_SHOW_PROGRESS", TRUE) &&
    interactive() &&
    !isTRUE(getOption("knitr.in.progress")) &&
    !isTRUE(getOption("rstudio.notebook.executing")) &&
    !testthat::is_testing()
}

#' @importFrom crayon blue cyan green bold reset col_nchar
pb_file_format <- function(filename) {
  glue::glue_col("{bold}indexing{reset} {blue}{basename(filename)}{reset} [:bar] {green}:rate{reset}, eta: {cyan}:eta{reset}")
}

pb_width <- function(format) {
  ansii_chars <- nchar(format) - col_nchar(format)
  getOption("width", 80L) + ansii_chars
}

pb_connection_format <- function(unused) {
  glue::glue_col("{bold}indexed{reset} {green}:bytes{reset} in {cyan}:elapsed{reset}, {green}:rate{reset}")
}

pb_write_format <- function(unused) {
  glue::glue_col("{bold}wrote{reset} {green}:bytes{reset} in {cyan}:elapsed{reset}, {green}:rate{reset}")
}

# Guess delimiter by splitting every line by each delimiter and choosing the
# delimiter which splits the lines into the highest number of consistent fields
guess_delim <- function(lines, delims = c(",", "\t", " ", "|", ":", ";", "\n")) {
  if (length(lines) == 0) {
    return("")
  }

  # blank text within quotes
  lines <- gsub('"[^"]+"', "", lines)

  splits <- lapply(delims, strsplit, x = lines, useBytes = TRUE, fixed = TRUE)

  counts <- lapply(splits, function(x) table(lengths(x)))

  choose_best <- function(i, j) {
    x <- counts[[i]]
    y <- counts[[j]]

    nx <- as.integer(names(counts[[i]]))
    ny <- as.integer(names(counts[[j]]))

    mx <- which.max(x)
    my <- which.max(y)

    if (x[[mx]] > y[[my]] ||
      x[[mx]] == y[[my]] && nx[[mx]] > ny[[my]]) {
      i
    } else {
      j
    }
  }
  res <- Reduce(choose_best, seq_along(counts))
  delims[[res]]
}


vroom_threads <- function() {
  as.integer(Sys.getenv("VROOM_THREADS", parallel::detectCores()))
}

vroom_tempfile <- function() {
  dir <- Sys.getenv("VROOM_TEMP_PATH")
  if (!nzchar(dir)) {
    dir <- tempdir()
  }
  tempfile(pattern = "vroom-", tmpdir = dir)
}

#' Show which column types are using Altrep
#'
#' `vroom_altrep_opts()` can be used directly as input to the `altrep_opts`
#' argument of [vroom()].
#'
#' Altrenatively there is also a family of environment variables to control use of
#' the Altrep framework. These can then be set in your `.Renviron` file, e.g.
#' with [usethis::edit_r_environ()]. For versions of R where the Altrep
#' framework is unavailable (R < 3.5.0) they are automatically turned off and
#' the variables have no effect. The variables can take one of `true`, `false`,
#' `TRUE`, `FALSE`, `1`, or `0`.
#'
#' - `VROOM_USE_ALTREP_NUMERICS` - If set use Altrep for _all_ numeric types
#'   (default `false`).
#'
#' There are also individual variables for each type. Currently only
#' `VROOM_USE_ALTREP_CHR` defaults to `true`.
#'
#' - `VROOM_USE_ALTREP_CHR`
#' - `VROOM_USE_ALTREP_FCT`
#' - `VROOM_USE_ALTREP_INT`
#' - `VROOM_USE_ALTREP_DBL`
#' - `VROOM_USE_ALTREP_NUM`
#' - `VROOM_USE_ALTREP_LGL`
#' - `VROOM_USE_ALTREP_DTTM`
#' - `VROOM_USE_ALTREP_DATE`
#' - `VROOM_USE_ALTREP_TIME`
#'
#' @param which A character vector of column types to use Altrep for. Can also
#'   take `TRUE` or `FALSE` to use Altrep for all possible or none of the
#'   types
#' @examples
#' vroom_altrep_opts()
#' vroom_altrep_opts(c("chr", "fct", "int"))
#' vroom_altrep_opts(TRUE)
#' vroom_altrep_opts(FALSE)
#' @export
vroom_altrep_opts <- function(which = NULL) {
  if (!is.null(which)) {
    if (is.logical(which)) {
      types <- names(altrep_opts_vals())
      if (isTRUE(which)) {
        which <- as.list(stats::setNames(rep(TRUE, length(types)), types))
      } else {
        which <- as.list(stats::setNames(rep(FALSE, length(types)), types))
      }
    } else {
      which <- match.arg(which, names(altrep_opts_vals()), several.ok = TRUE)
      which <- as.list(stats::setNames(rep(TRUE, length(which)), which))
    }
  }


  args <- list(
    which$chr %||% vroom_use_altrep_chr(),
    which$fct %||% vroom_use_altrep_fct(),
    which$int %||% vroom_use_altrep_int(),
    which$dbl %||% vroom_use_altrep_dbl(),
    which$num %||% vroom_use_altrep_num(),
    which$lgl %||% vroom_use_altrep_lgl(),
    which$dttm %||% vroom_use_altrep_dttm(),
    which$date %||% vroom_use_altrep_date(),
    which$time %||% vroom_use_altrep_time()
  )

  out <-  0L
  for (i in seq_along(args)) {
    out <- bitwOr(out, bitwShiftL(as.integer(args[[i]]), i - 1L))
  }
  structure(out, class = "vroom_altrep_opts")
}

altrep_opts_vals <- function() c(
  "none" = 0L,
  "chr" = 1L,
  "fct" = 2L,
  "int" = 4L,
  "dbl" = 8L,
  "num" = 16L,
  "lgl" = 32L,
  "dttm" = 64L,
  "date" = 128L,
  "time" = 256L
)

#' @export
print.vroom_altrep_opts <- function(x, ...) {
  vals <- altrep_opts_vals()
  reps <- names(vals)[bitwAnd(vals, x) > 0]

  cat("Using Altrep representations for:\n",
    glue::glue("
        * {reps}
       ", reps = glue::glue_collapse(reps, "\n * ")), "\n", sep = "")
}

vroom_use_altrep_chr <- function() {
  getRversion() > "3.5.0" && env_to_logical("VROOM_USE_ALTREP_CHR", TRUE)
}

vroom_use_altrep_fct <- function() {
  # fct is a numeric internally
  getRversion() > "3.5.0" && (env_to_logical("VROOM_USE_ALTREP_NUMERICS", FALSE) || env_to_logical("VROOM_USE_ALTREP_FCT", FALSE))
}

vroom_use_altrep_int <- function() {
  getRversion() > "3.5.0" && (env_to_logical("VROOM_USE_ALTREP_NUMERICS", FALSE) || env_to_logical("VROOM_USE_ALTREP_INT", FALSE))
}

vroom_use_altrep_dbl <- function() {
  getRversion() > "3.5.0" && (env_to_logical("VROOM_USE_ALTREP_NUMERICS", FALSE) || env_to_logical("VROOM_USE_ALTREP_DBL", FALSE))
}

vroom_use_altrep_num <- function() {
  getRversion() > "3.5.0" && (env_to_logical("VROOM_USE_ALTREP_NUMERICS", FALSE) || env_to_logical("VROOM_USE_ALTREP_NUM", FALSE))
}

vroom_use_altrep_lgl <- function() {
  getRversion() > "3.6.0" && (env_to_logical("VROOM_USE_ALTREP_NUMERICS", FALSE) || env_to_logical("VROOM_USE_ALTREP_LGL", FALSE))
}

vroom_use_altrep_dttm <- function() {
  getRversion() > "3.5.0" && (env_to_logical("VROOM_USE_ALTREP_NUMERICS", FALSE) || env_to_logical("VROOM_USE_ALTREP_DTTM", FALSE))
}

vroom_use_altrep_date <- function() {
  getRversion() > "3.5.0" && (env_to_logical("VROOM_USE_ALTREP_NUMERICS", FALSE) || env_to_logical("VROOM_USE_ALTREP_DATE", FALSE))
}

vroom_use_altrep_time <- function() {
  getRversion() > "3.5.0" && (env_to_logical("VROOM_USE_ALTREP_NUMERICS", FALSE) || env_to_logical("VROOM_USE_ALTREP_TIME", FALSE))
}
