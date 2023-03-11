#' Read a delimited file into a tibble
#'
#' @param file Either a path to a file, a connection, or literal data (either a
#'   single string or a raw vector).
#'
#'   Files ending in `.gz`, `.bz2`, `.xz`, or `.zip` will be automatically
#'   uncompressed. Files starting with `http://`, `https://`, `ftp://`, or
#'   `ftps://` will be automatically downloaded. Remote gz files can also be
#'   automatically downloaded and decompressed.
#'
#'   Literal data is most useful for examples and tests. To be recognised as
#'   literal data, the input must be either wrapped with `I()`, be a string
#'   containing at least one new line, or be a vector containing at least one
#'   string with a new line.
#' @param delim One or more characters used to delimit fields within a
#'   file. If `NULL` the delimiter is guessed from the set of `c(",", "\t", " ",
#'   "|", ":", ";")`.
#' @param col_names Either `TRUE`, `FALSE` or a character vector
#'   of column names.
#'
#'   If `TRUE`, the first row of the input will be used as the column
#'   names, and will not be included in the data frame. If `FALSE`, column
#'   names will be generated automatically: X1, X2, X3 etc.
#'
#'   If `col_names` is a character vector, the values will be used as the
#'   names of the columns, and the first row of the input will be read into
#'   the first row of the output data frame.
#'
#'   Missing (`NA`) column names will generate a warning, and be filled
#'   in with dummy names `...1`, `...2` etc. Duplicate column names
#'   will generate a warning and be made unique, see `name_repair` to control
#'   how this is done.
#' @param col_types One of `NULL`, a [cols()] specification, or
#'   a string.
#'
#'   If `NULL`, all column types will be imputed from `guess_max` rows
#'   on the input interspersed throughout the file. This is convenient (and
#'   fast), but not robust. If the imputation fails, you'll need to increase
#'   the `guess_max` or supply the correct types yourself.
#'
#'   Column specifications created by [list()] or [cols()] must contain
#'   one column specification for each column. If you only want to read a
#'   subset of the columns, use [cols_only()].
#'
#'   Alternatively, you can use a compact string representation where each
#'   character represents one column:
#' - c = character
#' - i = integer
#' - n = number
#' - d = double
#' - l = logical
#' - f = factor
#' - D = date
#' - T = date time
#' - t = time
#' - ? = guess
#' - _ or - = skip
#'
#'    By default, reading a file without a column specification will print a
#'    message showing what `readr` guessed they were. To remove this message,
#'    set `show_col_types = FALSE` or set `options(readr.show_col_types = FALSE).
#' @param id Either a string or 'NULL'. If a string, the output will contain a
#'   variable with that name with the filename(s) as the value. If 'NULL', the
#'   default, no variable will be created.
#' @param skip Number of lines to skip before reading data. If `comment` is
#'   supplied any commented lines are ignored _after_ skipping.
#' @param n_max Maximum number of lines to read.
#' @param na Character vector of strings to interpret as missing values. Set this
#'   option to `character()` to indicate no missing values.
#' @param quote Single character used to quote strings.
#' @param comment A string used to identify comments. Any text after the
#'   comment characters will be silently ignored.
#' @param skip_empty_rows Should blank rows be ignored altogether? i.e. If this
#'   option is `TRUE` then blank rows will not be represented at all.  If it is
#'   `FALSE` then they will be represented by `NA` values in all the columns.
#' @param trim_ws Should leading and trailing whitespace (ASCII spaces and tabs) be trimmed from
#'     each field before parsing it?
#' @param escape_double Does the file escape quotes by doubling them?
#'   i.e. If this option is `TRUE`, the value '""' represents
#'   a single quote, '"'.
#' @param escape_backslash Does the file use backslashes to escape special
#'   characters? This is more general than `escape_double` as backslashes
#'   can be used to escape the delimiter character, the quote character, or
#'   to add special characters like `\\n`.
#' @param locale The locale controls defaults that vary from place to place.
#'   The default locale is US-centric (like R), but you can use
#'   [locale()] to create your own locale that controls things like
#'   the default time zone, encoding, decimal mark, big mark, and day/month
#'   names.
#' @param guess_max Maximum number of lines to use for guessing column types.
#'   See `vignette("column-types", package = "readr")` for more details.
#' @param altrep Control which column types use Altrep representations,
#'   either a character vector of types, `TRUE` or `FALSE`. See
#'   [vroom_altrep()] for for full details.
#' @param altrep_opts \Sexpr[results=rd, stage=render]{lifecycle::badge("deprecated")}
#' @param col_select Columns to include in the results. You can use the same
#'   mini-language as `dplyr::select()` to refer to the columns by name. Use
#'   `c()` to use more than one selection expression. Although this
#'   usage is less common, `col_select` also accepts a numeric column index. See
#'   [`?tidyselect::language`][tidyselect::language] for full details on the
#'   selection language.
#' @param num_threads Number of threads to use when reading and materializing
#'   vectors. If your data contains newlines within fields the parser will
#'   automatically be forced to use a single thread only.
#' @param progress Display a progress bar? By default it will only display
#'   in an interactive session and not while knitting a document. The automatic
#'   progress bar can be disabled by setting option `readr.show_progress` to
#'   `FALSE`.
#' @param show_col_types Control showing the column specifications. If `TRUE`
#'   column specifications are always show, if `FALSE` they are never shown. If
#'   `NULL` (the default) they are shown only if an explicit specification is not
#'   given to `col_types`.
#' @param .name_repair Handling of column names. The default behaviour is to
#'   ensure column names are `"unique"`. Various repair strategies are
#'   supported:
#'   * `"minimal"`: No name repair or checks, beyond basic existence of names.
#'   * `"unique"` (default value): Make sure names are unique and not empty.
#'   * `"check_unique"`: no name repair, but check they are `unique`.
#'   * `"universal"`: Make the names `unique` and syntactic.
#'   * A function: apply custom name repair (e.g., `name_repair = make.names`
#'     for names in the style of base R).
#'   * A purrr-style anonymous function, see [rlang::as_function()].
#'
#'   This argument is passed on as `repair` to [vctrs::vec_as_names()].
#'   See there for more details on these terms and the strategies used
#'   to enforce them.
#' @export
#' @examples
#' # get path to example file
#' input_file <- vroom_example("mtcars.csv")
#' input_file
#'
#' # Read from a path
#'
#' # Input sources -------------------------------------------------------------
#' # Read from a path
#' vroom(input_file)
#' # You can also use paths directly
#' # vroom("mtcars.csv")
#'
#' \dontrun{
#' # Including remote paths
#' vroom("https://github.com/tidyverse/vroom/raw/main/inst/extdata/mtcars.csv")
#' }
#'
#' # Or directly from a string with `I()`
#' vroom(I("x,y\n1,2\n3,4\n"))
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
#' vroom(input_file, col_select = c(car = model, everything()))
#'
#' # Column types --------------------------------------------------------------
#' # By default, vroom guesses the columns types, looking at 1000 rows
#' # throughout the dataset.
#' # You can specify them explicitly with a compact specification:
#' vroom(I("x,y\n1,2\n3,4\n"), col_types = "dc")
#'
#' # Or with a list of column types:
#' vroom(I("x,y\n1,2\n3,4\n"), col_types = list(col_double(), col_character()))
#'
#' # File types ----------------------------------------------------------------
#' # csv
#' vroom(I("a,b\n1.0,2.0\n"), delim = ",")
#' # tsv
#' vroom(I("a\tb\n1.0\t2.0\n"))
#' # Other delimiters
#' vroom(I("a|b\n1.0|2.0\n"), delim = "|")
#'
#' # Read datasets across multiple files ---------------------------------------
#' mtcars_by_cyl <- vroom_example(vroom_examples("mtcars-"))
#' mtcars_by_cyl
#'
#' # Pass the filenames directly to vroom, they are efficiently combined
#' vroom(mtcars_by_cyl)
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
  skip_empty_rows = TRUE,
  trim_ws = TRUE,
  escape_double = TRUE,
  escape_backslash = FALSE,
  locale = default_locale(),
  guess_max = 100,
  altrep = TRUE,
  altrep_opts = deprecated(),
  num_threads = vroom_threads(),
  progress = vroom_progress(),
  show_col_types = NULL,
  .name_repair = "unique"
  ) {

  # vroom does not support newlines as the delimiter, just as the EOL, so just
  # assign a value that should never appear in CSV text as the delimiter,
  # 001, start of heading.
  if (identical(delim, "\n")) {
    delim <- "\x01"
  }

  if (!is_missing(altrep_opts)) {
    deprecate_warn("1.1.0", "vroom(altrep_opts = )", "vroom(altrep = )")
    altrep <- altrep_opts
  }

  file <- standardise_path(file)

  if (!is_ascii_compatible(locale$encoding)) {
    file <- reencode_file(file, locale$encoding)
    locale$encoding <- "UTF-8"
  }

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

  col_select <- vroom_enquo(enquo(col_select))

  has_col_types <- !is.null(col_types)

  col_types <- as.col_spec(col_types)

  na <- enc2utf8(na)

  out <- vroom_(file, delim = delim %||% col_types$delim, col_names = col_names,
    col_types = col_types, id = id, skip = skip, col_select = col_select,
    name_repair = .name_repair,
    na = na, quote = quote, trim_ws = trim_ws, escape_double = escape_double,
    escape_backslash = escape_backslash, comment = comment,
    skip_empty_rows = skip_empty_rows, locale = locale,
    guess_max = guess_max, n_max = n_max, altrep = vroom_altrep(altrep),
    num_threads = num_threads, progress = progress)

  # Drop any NULL columns
  is_null <- vapply(out, is.null, logical(1))
  out[is_null] <- NULL

  # If no rows expand columns to be the same length and names as the spec
  if (NROW(out) == 0) {
    cols <- attr(out, "spec")[["cols"]]
    for (i in seq_along(cols)) {
      out[[i]] <- collector_value(cols[[i]])
    }
    names(out) <- names(cols)
  }

  out <- tibble::as_tibble(out, .name_repair = identity)
  class(out) <- c("spec_tbl_df", class(out))

  out <- vroom_select(out, col_select, id)

  if (should_show_col_types(has_col_types, show_col_types)) {
    show_col_types(out, locale)
  }

  out
}

should_show_col_types <- function(has_col_types, show_col_types) {
  if (is.null(show_col_types)) {
    return(isTRUE(!has_col_types))
  }
  isTRUE(show_col_types)
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

#' Determine whether progress bars should be shown
#'
#' By default, vroom shows progress bars. However, progress reporting is
#' suppressed if any of the following conditions hold:
#' - The bar is explicitly disabled by setting the environment variable
#'   `VROOM_SHOW_PROGRESS` to `"false"`.
#' - The code is run in a non-interactive session, as determined by
#'   [rlang::is_interactive()].
#' - The code is run in an RStudio notebook chunk, as determined by
#'   `getOption("rstudio.notebook.executing")`.
#' @export
#' @examples
#' vroom_progress()
vroom_progress <- function() {
  env_to_logical("VROOM_SHOW_PROGRESS", TRUE) &&
    is_interactive() &&
    # some analysis re: rstudio.notebook.executing can be found in:
    # https://github.com/r-lib/rlang/issues/1031
    # TL;DR it's not consulted by is_interactive(), but probably should be
    # consulted for progress reporting specifically
    !isTRUE(getOption("rstudio.notebook.executing"))
}

pb_file_format <- function(filename) {

  # Workaround RStudio bug https://github.com/rstudio/rstudio/issues/4777
  withr::with_options(list(crayon.enabled = (!is_rstudio_console() || is_rstudio_version("1.2.1578")) && getOption("crayon.enabled", TRUE)),
    glue::glue_col("{bold}indexing{reset} {blue}{basename(filename)}{reset} [:bar] {green}:rate{reset}, eta: {cyan}:eta{reset}")
  )
}

pb_width <- function(format) {
  ansii_chars <- nchar(format) - crayon::col_nchar(format)
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

cached <- new.env(parent = emptyenv())

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
#' with `usethis::edit_r_environ()`. For versions of R where the Altrep
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
    getRversion() >= "3.5.0" && which$dbl %||% vroom_use_altrep_dbl(),
    getRversion() >= "3.5.0" && which$num %||% vroom_use_altrep_num(),
    getRversion() >= "3.6.0" && which$lgl %||% vroom_use_altrep_lgl(), # logicals only supported in R 3.6.0+
    getRversion() >= "3.5.0" && which$dttm %||% vroom_use_altrep_dttm(),
    getRversion() >= "3.5.0" && which$date %||% vroom_use_altrep_date(),
    getRversion() >= "3.5.0" && which$time %||% vroom_use_altrep_time(),
    getRversion() >= "3.5.0" && which$big_int %||% vroom_use_altrep_big_int()
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
  "big_int" = 512L,
  "skip" = 1024L
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
