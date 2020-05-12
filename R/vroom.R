#' @useDynLib vroom, .registration = TRUE
#' @importFrom Rcpp sourceCpp
#' @importFrom bit64 integer64
NULL

#' Read a delimited file into a tibble
#'
#' @inheritParams readr::read_delim
#' @param file path to a local file.
#' @param delim One or more characters used to delimit fields within a
#'   file. If `NULL` the delimiter is guessed from the set of `c(",", "\t", " ",
#'   "|", ":", ";")`.
#' @param num_threads Number of threads to use when reading and materializing
#'   vectors. If your data contains embedded newlines (newlines within fields)
#'   you _must_ use `num_threads = 1` to read the data properly.
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
#' @param altrep Control which column types use Altrep representations,
#'   either a character vector of types, `TRUE` or `FALSE`. See
#'   [vroom_altrep()] for for full details.
#' @param altrep_opts \Sexpr[results=rd, stage=render]{lifecycle::badge("deprecated")}
#' @export
#' @examples
#' # Show path to example file
#' input_file <- vroom_example("mtcars.csv")
#'
#' # Read from a path
#'
#' # Input sources -------------------------------------------------------------
#' # Read from a path
#' vroom(input_file)
#' # You can also use literal paths directly
#' # vroom("mtcars.csv")
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
#' vroom(input_file, col_select = c(model, cyl, gear))
#' vroom(input_file, col_select = c(1, 3, 11))
#'
#' # Or use the selection helpers
#' vroom(input_file, col_select = starts_with("d"))
#'
#' # You can also rename specific columns
#' vroom(input_file, col_select = list(car = model, everything()))
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
vroom <- function(
  file,
  delim = NULL,
  col_names = TRUE,
  col_types = NULL,
  col_select = NULL,
  id = NULL,
  skip = 0,
  n_max = Inf,
  na = c("", "NA"),
  quote = '"',
  comment = "",
  trim_ws = TRUE,
  escape_double = TRUE,
  escape_backslash = FALSE,
  locale = default_locale(),
  guess_max = 100,
  altrep = TRUE,
  altrep_opts = deprecated(),
  num_threads = vroom_threads(),
  progress = vroom_progress(),
  .name_repair = "unique"
  ) {

  if (!rlang::is_missing(altrep_opts)) {
    deprecate_warn("1.1.0", "vroom(altrep_opts = )", "vroom(altrep = )")
    altrep <- altrep_opts
  }

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

  has_spec <- !is.null(col_types)

  col_types <- as.col_spec(col_types)

  na <- enc2utf8(na)

  out <- vroom_(file, delim = delim %||% col_types$delim, col_names = col_names,
    col_types = col_types, id = id, skip = skip, col_select = col_select,
    na = na, quote = quote, trim_ws = trim_ws, escape_double = escape_double,
    escape_backslash = escape_backslash, comment = comment, locale = locale,
    guess_max = guess_max, n_max = n_max, altrep = vroom_altrep(altrep),
    num_threads = num_threads, progress = progress)

  out <- tibble::as_tibble(out, .name_repair = .name_repair)

  out <- vroom_select(out, col_select, id)

  if (!has_spec) {
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
    !(is_loaded("testthat") && testthat::is_testing())
}

#' @importFrom crayon blue cyan green bold reset col_nchar
pb_file_format <- function(filename) {

  # Workaround RStudio bug https://github.com/rstudio/rstudio/issues/4777
  withr::with_options(list(crayon.enabled = (!is_rstudio_console() || is_rstudio_version("1.2.1578")) && getOption("crayon.enabled", TRUE)),
    glue::glue_col("{bold}indexing{reset} {blue}{basename(filename)}{reset} [:bar] {green}:rate{reset}, eta: {cyan}:eta{reset}")
  )
}

pb_width <- function(format) {
  ansii_chars <- nchar(format) - col_nchar(format)
  getOption("width", 80L) + ansii_chars
}

pb_connection_format <- function(unused) {
  withr::with_options(list(crayon.enabled = (!is_rstudio_console() || is_rstudio_version("1.2.1578")) && getOption("crayon.enabled", TRUE)),
    glue::glue_col("{bold}indexed{reset} {green}:bytes{reset} in {cyan}:elapsed{reset}, {green}:rate{reset}")
  )
}

pb_write_format <- function(unused) {
  withr::with_options(list(crayon.enabled = (!is_rstudio_console() || is_rstudio_version("1.2.1578")) && getOption("crayon.enabled", TRUE)),
    glue::glue_col("{bold}wrote{reset} {green}:bytes{reset} in {cyan}:elapsed{reset}, {green}:rate{reset}")
  )
}

# Guess delimiter by splitting every line by each delimiter and choosing the
# delimiter which splits the lines into the highest number of consistent fields
guess_delim <- function(lines, delims = c(",", "\t", " ", "|", ":", ";")) {
  if (length(lines) == 0) {
    return("")
  }

  # blank text within quotes
  lines <- gsub('"[^"]*"', "", lines)

  splits <- lapply(delims, strsplit, x = lines, useBytes = TRUE, fixed = TRUE)

  counts <- lapply(splits, function(x) table(lengths(x)))

  num_fields <- vapply(counts, function(x) as.integer(names(x)[[1]]), integer(1))

  num_lines <- vapply(counts, function(x) (x)[[1]], integer(1))

  top_lines <- 0
  top_idx <- 0
  for (i in seq_along(delims)) {
    if (num_fields[[i]] >= 2 && num_lines[[i]] > top_lines ||
      (top_lines == num_lines[[i]] && (top_idx <= 0 || num_fields[[top_idx]] < num_fields[[i]]))) {
      top_lines <- num_lines[[i]]
      top_idx <- i
    }
  }
  if (top_idx == 0) {
    stop(glue::glue('
        Could not guess the delimiter.\n
        {silver("Use `vroom(delim =)` to specify one explicitly.")}
        '), call. = FALSE)
  }

  delims[[top_idx]]
}

cached <- new.env(emptyenv())

vroom_threads <- function() {
  res <- as.integer(
    Sys.getenv("VROOM_THREADS",
      cached$num_threads <- cached$num_threads %||% parallel::detectCores()
    )
  )
  if (is.na(res) || res <= 0) {
    res <- 1
  }
  res
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
#' `vroom_altrep()` can be used directly as input to the `altrep`
#' argument of [vroom()].
#'
#' Alternatively there is also a family of environment variables to control use of
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
#' - `VROOM_USE_ALTREP_BIG_INT`
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
#' vroom_altrep()
#' vroom_altrep(c("chr", "fct", "int"))
#' vroom_altrep(TRUE)
#' vroom_altrep(FALSE)
#' @export
vroom_altrep <- function(which = NULL) {
  if (!is.null(which)) {
    if (is.logical(which)) {
      types <- names(altrep_vals())
      if (isTRUE(which)) {
        which <- as.list(stats::setNames(rep(TRUE, length(types)), types))
      } else {
        which <- as.list(stats::setNames(rep(FALSE, length(types)), types))
      }
    } else {
      which <- match.arg(which, names(altrep_vals()), several.ok = TRUE)
      which <- as.list(stats::setNames(rep(TRUE, length(which)), which))
    }
  }


  args <- list(
    getRversion() >= "3.5.0" && which$chr %||% vroom_use_altrep_chr(),
    getRversion() >= "3.5.0" && which$fct %||% vroom_use_altrep_fct(),
    getRversion() >= "3.5.0" && which$int %||% vroom_use_altrep_int(),
    getRversion() >= "3.5.0" && which$int %||% vroom_use_altrep_big_int(),
    getRversion() >= "3.5.0" && which$dbl %||% vroom_use_altrep_dbl(),
    getRversion() >= "3.5.0" && which$num %||% vroom_use_altrep_num(),
    getRversion() >= "3.6.0" && which$lgl %||% vroom_use_altrep_lgl(), # logicals only supported in R 3.6.0+
    getRversion() >= "3.5.0" && which$dttm %||% vroom_use_altrep_dttm(),
    getRversion() >= "3.5.0" && which$date %||% vroom_use_altrep_date(),
    getRversion() >= "3.5.0" && which$time %||% vroom_use_altrep_time()
  )

  out <-  0L
  for (i in seq_along(args)) {
    out <- bitwOr(out, bitwShiftL(as.integer(args[[i]]), i - 1L))
  }
  structure(out, class = "vroom_altrep")
}

#' Show which column types are using Altrep
#'
#' @description
#' \Sexpr[results=rd, stage=render]{lifecycle::badge("deprecated")}
#' This function is deprecated in favor of `vroom_altrep()`.
#'
#' @inheritParams vroom_altrep
#' @export
vroom_altrep_opts <- function(which = NULL) {
  deprecate_warn("1.1.0", "vroom_altrep_opts()", "vroom_altrep()")
  vroom_altrep(which)
}

altrep_vals <- function() c(
  "none" = 0L,
  "chr" = 1L,
  "fct" = 2L,
  "int" = 4L,
  "dbl" = 8L,
  "num" = 16L,
  "lgl" = 32L,
  "dttm" = 64L,
  "date" = 128L,
  "time" = 256L,
# "skip" = 512L
  "big_int" = 1024L
)

#' @export
print.vroom_altrep <- function(x, ...) {
  vals <- altrep_vals()
  reps <- names(vals)[bitwAnd(vals, x) > 0]

  cat("Using Altrep representations for:\n",
    glue::glue("
        * {reps}
       ", reps = glue::glue_collapse(reps, "\n * ")), "\n", sep = "")
}

vroom_use_altrep_chr <- function() {
  env_to_logical("VROOM_USE_ALTREP_CHR", TRUE)
}

vroom_use_altrep_fct <- function() {
  # fct is a numeric internally
  env_to_logical("VROOM_USE_ALTREP_NUMERICS", FALSE) || env_to_logical("VROOM_USE_ALTREP_FCT", FALSE)
}

vroom_use_altrep_int <- function() {
  env_to_logical("VROOM_USE_ALTREP_NUMERICS", FALSE) || env_to_logical("VROOM_USE_ALTREP_INT", FALSE)
}

vroom_use_altrep_big_int <- function() {
  env_to_logical("VROOM_USE_ALTREP_NUMERICS", FALSE) || env_to_logical("VROOM_USE_ALTREP_BIG_INT", FALSE)
}

vroom_use_altrep_dbl <- function() {
  env_to_logical("VROOM_USE_ALTREP_NUMERICS", FALSE) || env_to_logical("VROOM_USE_ALTREP_DBL", FALSE)
}

vroom_use_altrep_num <- function() {
  env_to_logical("VROOM_USE_ALTREP_NUMERICS", FALSE) || env_to_logical("VROOM_USE_ALTREP_NUM", FALSE)
}

vroom_use_altrep_lgl <- function() {
  env_to_logical("VROOM_USE_ALTREP_NUMERICS", FALSE) || env_to_logical("VROOM_USE_ALTREP_LGL", FALSE)
}

vroom_use_altrep_dttm <- function() {
  env_to_logical("VROOM_USE_ALTREP_NUMERICS", FALSE) || env_to_logical("VROOM_USE_ALTREP_DTTM", FALSE)
}

vroom_use_altrep_date <- function() {
  env_to_logical("VROOM_USE_ALTREP_NUMERICS", FALSE) || env_to_logical("VROOM_USE_ALTREP_DATE", FALSE)
}

vroom_use_altrep_time <- function() {
  env_to_logical("VROOM_USE_ALTREP_NUMERICS", FALSE) || env_to_logical("VROOM_USE_ALTREP_TIME", FALSE)
}
