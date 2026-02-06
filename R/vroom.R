#' Read a delimited file into a tibble
#'
#' @param file Either a path to a file, a connection, or literal data (either a
#'   single string or a raw vector). `file` can also be a character vector
#'   containing multiple filepaths or a list containing multiple connections.
#'
#'   Files ending in `.gz`, `.bz2`, `.xz`, or `.zip` will be automatically
#'   uncompressed. Files starting with `http://`, `https://`, `ftp://`, or
#'   `ftps://` will be automatically downloaded. Remote `.gz` files can also be
#'   automatically downloaded and decompressed.
#'
#'   Literal data is most useful for examples and tests. To be recognised as
#'   literal data, wrap the input with `I()`.
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
#'   If `NULL`, all column types will be inferred from `guess_max` rows
#'   of the input, interspersed throughout the file. This is convenient (and
#'   fast), but not robust. If the guessed types are wrong, you'll need to
#'   increase `guess_max` or supply the correct types yourself.
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
#' By default, reading a file without a column specification will print a
#' message showing the guessed types. To suppress this message, set
#' `show_col_types = FALSE`.
#' @param id Either a string or 'NULL'. If a string, the output will contain a
#'   column with that name with the filename(s) as the value, i.e. this column
#'   effectively tells you the source of each row. If 'NULL' (the default), no
#'   such column will be created.
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
#'   in an interactive session and not while executing in an RStudio notebook
#'   chunk. The display of the progress bar can be disabled by setting the
#'   environment variable `VROOM_SHOW_PROGRESS` to `"false"`.
#' @param show_col_types Control showing the column specifications. If `TRUE`
#'   column specifications are always shown, if `FALSE` they are never shown. If
#'   `NULL` (the default), they are shown only if an explicit specification is
#'   not given in `col_types`, i.e. if the types have been guessed.
#' @param .name_repair Handling of column names. The default behaviour is to
#'   ensure column names are `"unique"`. Various repair strategies are
#'   supported:
#'   * `"minimal"`: No name repair or checks, beyond basic existence of names.
#'   * `"unique"` (default value): Make sure names are unique and not empty.
#'   * `"check_unique"`: No name repair, but check they are `unique`.
#'   * `"unique_quiet"`: Repair with the `unique` strategy, quietly.
#'   * `"universal"`: Make the names `unique` and syntactic.
#'   * `"universal_quiet"`: Repair with the `universal` strategy, quietly.
#'   * A function: Apply custom name repair (e.g., `name_repair = make.names`
#'     for names in the style of base R).
#'   * A purrr-style anonymous function, see [rlang::as_function()].
#'
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
#'
#' # If you need to extract data from the filenames, use `id` to request a
#' # column that reveals the underlying file path
#' dat <- vroom(mtcars_by_cyl, id = "source")
#' dat$source <- basename(dat$source)
#' dat
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

  file <- standardise_path(file)

  if (length(file) == 0 || (n_max == 0 && identical(col_names, FALSE))) {
    return(tibble::tibble())
  }

  # When n_max = 0 with explicit col_names, return 0-row tibble with
  # the requested column structure (no need to read the file at all).
  if (n_max == 0 && is.character(col_names)) {
    out <- tibble::as_tibble(
      stats::setNames(
        rep(list(character(0)), length(col_names)),
        col_names
      )
    )
    return(out)
  }

  col_select <- vroom_enquo(enquo(col_select))

  na_str <- paste(na, collapse = ",")

  ct <- resolve_libvroom_col_types(col_types)
  col_types_int <- ct$col_types_int
  col_type_names <- ct$col_type_names
  resolved_spec <- ct$resolved_spec

  # Extract .delim from col_spec when the user hasn't specified delim explicitly
  if (
    is.null(delim) &&
      !is.null(resolved_spec) &&
      !is.null(resolved_spec$delim) &&
      nzchar(resolved_spec$delim)
  ) {
    delim <- resolved_spec$delim
  }

  # When col_names is not TRUE, libvroom uses V1, V2, ... internally.
  # Translate user-provided col_type_names to match libvroom's V-names.
  libvroom_col_type_names <- col_type_names
  if (!isTRUE(col_names) && length(col_type_names) > 0) {
    if (is.character(col_names)) {
      r_to_v <- setNames(paste0("V", seq_along(col_names)), col_names)
      matched <- r_to_v[col_type_names]
      libvroom_col_type_names[!is.na(matched)] <- matched[!is.na(matched)]
    } else if (isFALSE(col_names)) {
      # col_names = FALSE: R uses X1, X2, ... but libvroom uses V1, V2, ...
      x_pattern <- grepl("^X([0-9]+)$", col_type_names)
      if (any(x_pattern)) {
        libvroom_col_type_names[x_pattern] <- sub(
          "^X",
          "V",
          col_type_names[x_pattern]
        )
      }
    }
  }

  # Convert .default to libvroom int for columns without explicit types
  default_col_type <- 0L
  if (
    !is.null(resolved_spec) &&
      !is.null(resolved_spec$default) &&
      !inherits(resolved_spec$default, "collector_guess")
  ) {
    default_col_type <- collector_to_libvroom_int(resolved_spec$default)
  }

  # Helper to read a single file/connection through libvroom
  read_one_libvroom <- function(input) {
    if (inherits(input, "connection")) {
      input <- read_connection_raw(input)
      if (length(input) == 0L) {
        return(NULL)
      }
    }

    one <- tryCatch(
      vroom_libvroom_(
        input = input,
        delim = delim %||% "",
        quote = quote,
        has_header = isTRUE(col_names),
        skip = as.integer(skip),
        comment = comment,
        skip_empty_rows = skip_empty_rows,
        trim_ws = trim_ws,
        na_values = na_str,
        num_threads = as.integer(num_threads),
        strings_as_factors = FALSE,
        use_altrep = if (is.character(altrep)) {
          "chr" %in% altrep
        } else {
          isTRUE(altrep)
        },
        col_types = col_types_int,
        col_type_names = libvroom_col_type_names,
        default_col_type = default_col_type,
        escape_backslash = escape_backslash,
        decimal_mark = locale$decimal_mark,
        guess_max = if (is.infinite(guess_max)) -1L else as.integer(guess_max)
      ),
      error = function(e) {
        msg <- conditionMessage(e)
        if (
          grepl("All data was skipped", msg, fixed = TRUE) ||
            grepl("Header row is empty", msg, fixed = TRUE) ||
            grepl("File contains only comment lines", msg, fixed = TRUE) ||
            grepl("Empty file", msg, fixed = TRUE)
        ) {
          NULL
        } else {
          stop(e)
        }
      }
    )

    if (is.null(one)) {
      return(NULL)
    }

    # For empty results with user-provided col_names, ensure correct structure
    if (
      nrow(one) == 0 &&
        is.character(col_names) &&
        length(col_names) > ncol(one)
    ) {
      one <- tibble::as_tibble(stats::setNames(
        rep(list(character(0L)), length(col_names)),
        col_names
      ))
    }

    # Determine if col_names should be applied before or after skip filtering.
    # When col_names has fewer entries than file columns and there are positional
    # skips, col_names are intended for the *kept* columns only, so we defer.
    has_positional_skips <- length(col_types_int) > 0 &&
      length(col_type_names) == 0 &&
      any(col_types_int == -1L)
    defer_col_names <- is.character(col_names) &&
      has_positional_skips &&
      length(col_names) < ncol(one)

    # Apply col_names renaming for non-TRUE col_names (unless deferred)
    if (!defer_col_names) {
      if (is.character(col_names)) {
        names(one) <- make_names(col_names, ncol(one))
      } else if (isFALSE(col_names)) {
        names(one) <- make_names(character(), ncol(one))
      }
    }

    # Warn about mismatched column names in named col_types
    if (
      length(col_type_names) > 0 &&
        !is.null(resolved_spec) &&
        !inherits(resolved_spec$default, "collector_skip")
    ) {
      bad_types <- !(col_type_names %in% names(one))
      if (any(bad_types)) {
        warn(
          paste0(
            "The following named parsers don't match the column names: ",
            paste0(col_type_names[bad_types], collapse = ", ")
          ),
          class = "vroom_mismatched_column_name"
        )
      }
    }

    one <- filter_cols_only_and_skip(
      one,
      resolved_spec,
      col_types_int,
      col_type_names
    )

    # Apply deferred col_names after skip filtering
    if (defer_col_names) {
      names(one) <- make_names(col_names, ncol(one))
    }

    # Apply R-side post-processing for types libvroom parsed as STRING
    one <- apply_col_postprocessing(
      one,
      resolved_spec,
      col_types_int,
      col_type_names,
      locale
    )

    # Adjust timezone for TIMESTAMP columns if locale specifies non-UTC tz.
    # libvroom always parses timestamps as UTC, so we need to re-interpret
    # the wall-clock time in the target timezone.
    locale_tz <- locale$tz %||% "UTC"
    if (identical(locale_tz, "")) {
      locale_tz <- Sys.timezone()
    }
    if (!identical(locale_tz, "UTC")) {
      for (i in seq_along(one)) {
        if (
          inherits(one[[i]], "POSIXct") &&
            identical(attr(one[[i]], "tzone"), "UTC")
        ) {
          # Re-interpret: the numeric value represents wall-clock time in UTC,
          # but it should be wall-clock time in the target timezone.
          utc_str <- format(
            one[[i]],
            format = "%Y-%m-%d %H:%M:%OS6",
            tz = "UTC"
          )
          one[[i]] <- as.POSIXct(
            utc_str,
            format = "%Y-%m-%d %H:%M:%OS",
            tz = locale_tz
          )
        }
      }
    }

    # Extract problems from C++ result (attached by vroom_libvroom_)
    probs <- attr(one, "problems")
    if (is.data.frame(probs) && nrow(probs) > 0) {
      file_path <- if (is.character(input)) input else "<connection>"
      probs$file <- rep(file_path, nrow(probs))
    } else {
      probs <- NULL
    }
    # Remove C++-attached problems (will be re-attached by finalize)
    attr(one, "problems") <- NULL

    list(
      data = one,
      problems = probs,
      resolved_spec = resolved_spec,
      col_types_int = col_types_int
    )
  }

  # Transcode non-UTF-8 files to UTF-8 before libvroom processes them
  file_encoding <- locale$encoding
  if (
    !identical(file_encoding, "UTF-8") &&
      !is_ascii_compatible(file_encoding) &&
      length(file) > 1
  ) {
    cli::cli_abort(c(
      "!" = "Reading multiple files with encoding {.val {file_encoding}} is not supported.",
      "i" = "Only ASCII-compatible encodings work with multiple files.",
      "i" = "Try reading each file separately."
    ))
  }
  if (!identical(file_encoding, "UTF-8")) {
    vroom_env <- environment()
    file <- lapply(file, function(input) {
      if (is.character(input)) {
        reencode_one_file(input, file_encoding, vroom_env)
      } else if (inherits(input, "connection")) {
        reencode_one_connection(input, file_encoding, vroom_env)
      } else {
        input
      }
    })
  }

  # Read each file and collect results
  results <- list()
  first_result <- NULL
  for (input in file) {
    # Skip truly empty files (0 bytes, no header)
    if (
      is.character(input) &&
        !is_url(input) &&
        file.exists(input) &&
        file.size(input) == 0
    ) {
      next
    }

    # Preserve original path for id column before converting to connection
    original_path <- if (is.character(input)) input else "<connection>"

    # Route URLs and compressed files through R connections for
    # decompression/download; plain local files pass through as paths so
    # libvroom can memory-map them directly.
    if (is.character(input) && (is_url(input) || is_compressed_path(input))) {
      input <- connection_or_filepath(input)
    }
    # Non-ASCII paths need to go through R connection for proper encoding
    # handling (libvroom expects UTF-8 paths but non-UTF-8 locales mangle them)
    if (is.character(input) && !is_ascii_path(input)) {
      input <- file(input)
    }

    res <- read_one_libvroom(input)
    if (is.null(res)) {
      next
    }

    # Keep the first result for column name/type info even if 0 rows
    if (is.null(first_result)) {
      first_result <- res
    }

    if (nrow(res$data) > 0) {
      # Add id column if requested
      if (!is.null(id)) {
        file_path <- original_path
        res$data <- cbind(
          stats::setNames(
            data.frame(
              rep(file_path, nrow(res$data)),
              stringsAsFactors = FALSE
            ),
            id
          ),
          res$data
        )
      }
      results[[length(results) + 1L]] <- res
    }
  }

  # If no results at all, build empty tibble with appropriate structure
  if (is.null(first_result)) {
    if (is.character(col_names) && length(col_names) > 0) {
      # Build empty tibble with correct column names and types from col_types
      empty_cols <- stats::setNames(
        lapply(col_names, function(nm) character(0L)),
        col_names
      )
      # Apply col_types if specified
      if (!is.null(resolved_spec) && length(resolved_spec$cols) > 0) {
        spec_names <- names(resolved_spec$cols)
        for (i in seq_along(resolved_spec$cols)) {
          col <- resolved_spec$cols[[i]]
          # Match by position or name
          target <- if (!is.null(spec_names) && nzchar(spec_names[[i]])) {
            match(spec_names[[i]], col_names)
          } else if (i <= length(col_names)) {
            i
          } else {
            NA_integer_
          }
          if (!is.na(target) && target <= length(col_names)) {
            empty_cols[[target]] <- collector_value(col)
          }
        }
        # Remove skipped columns
        keep <- vapply(empty_cols, Negate(is.null), logical(1))
        empty_cols <- empty_cols[keep]
      }
      return(tibble::as_tibble(empty_cols))
    } else if (!is.null(resolved_spec) && length(resolved_spec$cols) > 0) {
      # col_types specified with no col_names: use positional names
      spec_cols <- resolved_spec$cols
      non_skip <- !vapply(spec_cols, inherits, logical(1), "collector_skip")
      kept_cols <- spec_cols[non_skip]
      if (sum(non_skip) == 0) {
        return(tibble::tibble())
      }
      nms <- if (!is.null(names(kept_cols)) && any(nzchar(names(kept_cols)))) {
        names(kept_cols)
      } else {
        # Use original positions for X-names (e.g., "c-d" -> X1, X3)
        paste0("X", which(non_skip))
      }
      empty_cols <- stats::setNames(
        lapply(kept_cols, collector_value),
        nms
      )
      return(tibble::as_tibble(empty_cols))
    }
    return(tibble::tibble())
  }

  # Combine results
  if (length(results) == 0) {
    # All files were empty (header-only); use first_result for structure
    out <- first_result$data
    if (!is.null(id)) {
      out <- cbind(
        stats::setNames(
          data.frame(character(0), stringsAsFactors = FALSE),
          id
        ),
        out
      )
    }
  } else if (length(results) == 1) {
    out <- results[[1]]$data
  } else {
    out <- vctrs::vec_rbind(!!!lapply(results, function(r) r$data))
  }

  out <- tibble::as_tibble(out, .name_repair = .name_repair)

  # Build and attach spec attribute BEFORE col_select so it reflects
  # the full file schema, not just selected columns.
  # Exclude the id column from the spec column names.
  all_col_names <- setdiff(names(out), id)
  # If delimiter was auto-detected (delim is still NULL), try to infer it
  # from the column names for the spec attribute.
  spec_delim <- delim %||% ""
  if (!nzchar(spec_delim) && length(all_col_names) > 1) {
    # Try to infer the delimiter from the first input file
    spec_delim <- tryCatch(
      {
        first_input <- file[[1]]
        if (is.character(first_input) && file.exists(first_input)) {
          lines <- readLines(first_input, n = 5, warn = FALSE)
        } else if (is.raw(first_input)) {
          lines <- strsplit(
            rawToChar(first_input[seq_len(min(
              2000,
              length(first_input)
            ))]),
            "\n",
            fixed = TRUE
          )[[1]]
        } else {
          lines <- character()
        }
        if (length(lines) > 0) {
          guess_delim(lines)
        } else {
          ""
        }
      },
      error = function(e) ""
    )
  }
  attr(out, "spec") <- build_libvroom_spec(
    out[all_col_names],
    first_result$resolved_spec,
    first_result$col_types_int,
    all_col_names,
    delim = spec_delim
  )

  out <- apply_libvroom_col_select(out, col_select, id)

  # Apply n_max row limit (R-side truncation)
  if (!is.infinite(n_max) && n_max >= 0 && nrow(out) > n_max) {
    out <- out[seq_len(n_max), , drop = FALSE]
  }

  # Combine problems from all files
  all_problems <- do.call(rbind, lapply(results, function(r) r$problems))
  if (is.null(all_problems)) {
    all_problems <- tibble::tibble(
      row = integer(),
      col = integer(),
      expected = character(),
      actual = character(),
      file = character()
    )
  }

  if (!is.null(all_problems) && nrow(all_problems) > 0) {
    cli::cli_warn(
      c(
        "w" = "One or more parsing issues, call {.fun problems} on your data frame for details, e.g.:",
        " " = "dat <- vroom(...)",
        " " = "problems(dat)"
      ),
      class = "vroom_parse_issue"
    )
  }

  out <- finalize_libvroom_result(out, all_problems)

  has_col_types <- !is.null(col_types) && !identical(col_types, list())
  if (should_show_col_types(has_col_types, show_col_types)) {
    show_col_types(out, locale)
  }

  out
}


# Map an R col_spec to a vector of libvroom DataType integers.
#
# Mapping (matches libvroom::DataType enum):
#   0 = UNKNOWN (guess), 1 = BOOL, 2 = INT32, 3 = INT64
#   4 = FLOAT64, 5 = STRING, 6 = DATE, 7 = TIMESTAMP, -1 = skip
# Convert a single collector to a libvroom DataType integer.
# Returns: -1 (skip), 0 (guess), 1 (BOOL), 2 (INT32), 4 (FLOAT64),
#          5 (STRING, needs R post-processing), 6 (DATE), 7 (TIMESTAMP)
collector_to_libvroom_int <- function(collector) {
  cls <- class(collector)[[1]]
  switch(
    cls,
    collector_skip = -1L,
    collector_guess = 0L,
    collector_logical = 5L,
    collector_integer = 2L,
    collector_big_integer = 5L,
    collector_double = 4L,
    collector_character = 5L,
    collector_number = 5L,
    collector_time = 5L,
    collector_factor = 5L,
    collector_date = {
      if (
        identical(collector$format, "") ||
          identical(collector$format, "%AD")
      ) {
        6L
      } else {
        5L
      }
    },
    collector_datetime = {
      if (
        identical(collector$format, "") ||
          identical(collector$format, "%AD")
      ) {
        7L
      } else {
        5L
      }
    },
    5L
  )
}

col_types_to_libvroom <- function(spec) {
  vapply(spec$cols, collector_to_libvroom_int, integer(1))
}


# Build a col_spec from libvroom output for the spec attribute.
# If resolved_spec is provided, use it. Otherwise infer from R column types.
build_libvroom_spec <- function(
  out,
  resolved_spec,
  col_types_int,
  all_col_names,
  delim
) {
  if (!is.null(resolved_spec)) {
    spec_out <- resolved_spec
    if (length(spec_out$cols) == 0 && length(all_col_names) > 0) {
      # Pure .default spec: expand to all columns
      spec_out$cols <- rep(list(spec_out$default), length(all_col_names))
      names(spec_out$cols) <- all_col_names
    } else if (length(all_col_names) > 0 && length(spec_out$cols) > 0) {
      if (is.null(names(spec_out$cols)) || all(names(spec_out$cols) == "")) {
        # Positional spec: assign column names from the file
        if (length(spec_out$cols) <= length(all_col_names)) {
          names(spec_out$cols) <- all_col_names[seq_along(spec_out$cols)]
        }
      } else {
        # Named spec: fill in defaults for unspecified columns
        for (nm in all_col_names) {
          if (!(nm %in% names(spec_out$cols))) {
            spec_out$cols[[nm]] <- spec_out$default
          }
        }
        # Reorder to match file column order
        spec_out$cols <- spec_out$cols[intersect(
          all_col_names,
          names(spec_out$cols)
        )]
      }
    }
    spec_out$delim <- delim
    return(spec_out)
  }

  # No spec provided: build from R output types
  spec_cols <- lapply(out, function(col) {
    if (is.integer(col)) {
      col_integer()
    } else if (is.double(col)) {
      if (inherits(col, "Date")) {
        col_date()
      } else if (inherits(col, "POSIXct")) {
        col_datetime()
      } else {
        col_double()
      }
    } else if (is.logical(col)) {
      col_logical()
    } else {
      col_character()
    }
  })
  names(spec_cols) <- names(out)
  structure(
    list(cols = spec_cols, default = col_guess(), delim = delim),
    class = "col_spec"
  )
}

# Apply R-side type coercion for types libvroom parsed as STRING
apply_col_postprocessing <- function(
  out,
  spec,
  col_types_int,
  col_type_names,
  locale = default_locale()
) {
  out_names <- names(out)

  if (!is.null(spec)) {
    if (length(col_type_names) > 0) {
      # Named spec: match by name
      for (i in seq_along(spec$cols)) {
        col_name <- names(spec$cols)[[i]]
        out_idx <- match(col_name, out_names)
        if (is.na(out_idx)) {
          next
        }
        type_int <- col_types_int[[i]]
        if (type_int != 5L) {
          next
        }

        collector <- spec$cols[[i]]
        if (inherits(collector, "collector_character")) {
          next
        }

        out[[out_idx]] <- apply_collector(out[[out_idx]], collector, locale)
      }
    } else {
      # Positional spec: match by position
      out_col <- 0L
      for (i in seq_along(spec$cols)) {
        type_int <- col_types_int[[i]]
        if (type_int == -1L) {
          next
        }
        out_col <- out_col + 1L
        if (out_col > ncol(out)) {
          next
        }
        if (type_int != 5L) {
          next
        }

        collector <- spec$cols[[i]]
        if (inherits(collector, "collector_character")) {
          next
        }

        out[[out_col]] <- apply_collector(out[[out_col]], collector, locale)
      }
    }

    # Apply .default post-processing to columns not explicitly in spec$cols
    if (
      !is.null(spec$default) &&
        !inherits(spec$default, "collector_guess") &&
        !inherits(spec$default, "collector_skip") &&
        !inherits(spec$default, "collector_character")
    ) {
      default_int <- collector_to_libvroom_int(spec$default)
      if (default_int == 5L) {
        spec_col_names <- names(spec$cols)
        for (i in seq_along(out_names)) {
          if (!(out_names[[i]] %in% spec_col_names)) {
            out[[i]] <- apply_collector(out[[i]], spec$default, locale)
          }
        }
      }
    }
  }

  # Locale date_format guessing: when a non-default date_format is set,
  # try parsing type-guessed character columns as dates.
  date_format <- locale$date_format %||% ""
  if (nzchar(date_format) && !identical(date_format, "%AD")) {
    # Determine which columns were explicitly typed
    explicit_cols <- character()
    if (!is.null(spec) && length(spec$cols) > 0) {
      explicit_cols <- names(spec$cols)
    }

    for (i in seq_along(out_names)) {
      if (out_names[[i]] %in% explicit_cols) {
        next
      }
      if (!is.character(out[[i]])) {
        next
      }
      parsed <- tryCatch(
        parse_date_(out[[i]], date_format, locale),
        error = function(e) NULL
      )
      if (!is.null(parsed) && !all(is.na(parsed))) {
        out[[i]] <- parsed
      }
    }
  }

  out
}

apply_collector <- function(x, collector, locale = default_locale()) {
  cls <- class(collector)[[1]]
  switch(
    cls,
    collector_logical = {
      # libvroom only recognizes "TRUE"/"FALSE"; R/readr also accept
      # "T"/"F", "true"/"false", and "1"/"0".
      out <- rep(NA, length(x))
      upper <- toupper(x)
      out[upper %in% c("TRUE", "T", "1")] <- TRUE
      out[upper %in% c("FALSE", "F", "0")] <- FALSE
      as.logical(out)
    },
    collector_number = {
      parse_number_value(
        x,
        grouping_mark = locale$grouping_mark,
        decimal_mark = locale$decimal_mark
      )
    },
    collector_big_integer = {
      bit64::as.integer64(x)
    },
    collector_factor = {
      lvls <- collector$levels
      include_na <- isTRUE(collector$include_na)
      if (is.null(lvls)) {
        # Preserve first-appearance order (not alphabetical)
        if (include_na) {
          lvls <- unique(x)
        } else {
          lvls <- unique(x[!is.na(x)])
        }
        factor(x, levels = lvls, ordered = collector$ordered, exclude = NULL)
      } else {
        exclude <- if (include_na || anyNA(lvls)) NULL else NA
        factor(x, levels = lvls, ordered = collector$ordered, exclude = exclude)
      }
    },
    collector_time = {
      tryCatch(
        parse_time_(x, collector$format %||% "", locale),
        error = function(e) hms::as_hms(rep(NA_real_, length(x)))
      )
    },
    collector_date = {
      tryCatch(
        parse_date_(x, collector$format %||% "", locale),
        error = function(e) {
          as.Date(rep(NA_real_, length(x)), origin = "1970-01-01")
        }
      )
    },
    collector_datetime = {
      fmt <- collector$format %||% ""
      if (identical(fmt, "%s")) {
        # Epoch seconds: not in DateTimeParser, handle directly
        out <- .POSIXct(as.numeric(x), tz = "UTC")
        out[is.na(x)] <- .POSIXct(NA_real_, tz = "UTC")
        out
      } else {
        tryCatch(
          parse_datetime_(x, fmt, locale),
          error = function(e) .POSIXct(rep(NA_real_, length(x)), tz = "UTC")
        )
      }
    },
    x
  )
}

# Parse number values like readr's parse_number(): strip grouping marks,
# then extract the first valid number from each string.
parse_number_value <- function(x, grouping_mark = ",", decimal_mark = ".") {
  # Preserve NAs
  is_na <- is.na(x)

  # Remove grouping marks
  if (nzchar(grouping_mark)) {
    x <- gsub(grouping_mark, "", x, fixed = TRUE)
  }

  # Normalize decimal mark to "." before regex extraction
  if (!identical(decimal_mark, ".")) {
    x <- gsub(decimal_mark, ".", x, fixed = TRUE)
  }

  # Extract the first valid number (integer, decimal, or scientific notation)
  pattern <- "[+-]?[0-9]*\\.?[0-9]+([eE][+-]?[0-9]+)?"
  m <- regmatches(x, regexpr(pattern, x))

  # regmatches drops entries with no match; rebuild to original length
  result <- rep(NA_real_, length(x))
  has_match <- grepl(pattern, x)
  result[has_match & !is_na] <- as.double(m)
  result[is_na] <- NA_real_

  result
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
  withr::with_options(
    list(
      crayon.enabled = (!is_rstudio_console() ||
        is_rstudio_version("1.2.1578")) &&
        getOption("crayon.enabled", TRUE)
    ),
    glue::glue_col(
      "{bold}indexing{reset} {blue}{basename(filename)}{reset} [:bar] {green}:rate{reset}, eta: {cyan}:eta{reset}"
    )
  )
}

pb_width <- function(format) {
  ansii_chars <- nchar(format) - crayon::col_nchar(format)
  getOption("width", 80L) + ansii_chars
}

pb_connection_format <- function(unused) {
  withr::with_options(
    list(
      crayon.enabled = (!is_rstudio_console() ||
        is_rstudio_version("1.2.1578")) &&
        getOption("crayon.enabled", TRUE)
    ),
    glue::glue_col(
      "{bold}indexed{reset} {green}:bytes{reset} in {cyan}:elapsed{reset}, {green}:rate{reset}"
    )
  )
}

pb_write_format <- function(unused) {
  withr::with_options(
    list(
      crayon.enabled = (!is_rstudio_console() ||
        is_rstudio_version("1.2.1578")) &&
        getOption("crayon.enabled", TRUE)
    ),
    glue::glue_col(
      "{bold}wrote{reset} {green}:bytes{reset} in {cyan}:elapsed{reset}, {green}:rate{reset}"
    )
  )
}

# Guess delimiter by splitting every line by each delimiter and choosing the
# delimiter which splits the lines into the highest number of consistent fields.
# This looks like dead code on the R side, but it's called from C++.
guess_delim <- function(lines, delims = c(",", "\t", " ", "|", ":", ";")) {
  if (length(lines) == 0) {
    return("")
  }

  # blank text within quotes
  lines <- gsub('"[^"]*"', "", lines)

  splits <- lapply(delims, strsplit, x = lines, useBytes = TRUE, fixed = TRUE)

  counts <- lapply(splits, function(x) table(lengths(x)))

  num_fields <- vapply(
    counts,
    function(x) as.integer(names(x)[[1]]),
    integer(1)
  )

  num_lines <- vapply(counts, function(x) (x)[[1]], integer(1))

  top_lines <- 0
  top_idx <- 0
  for (i in seq_along(delims)) {
    if (
      num_fields[[i]] >= 2 &&
        num_lines[[i]] > top_lines ||
        (top_lines == num_lines[[i]] &&
          (top_idx <= 0 || num_fields[[top_idx]] < num_fields[[i]]))
    ) {
      top_lines <- num_lines[[i]]
      top_idx <- i
    }
  }
  if (top_idx == 0) {
    cli::cli_abort(c(
      "Could not guess the delimiter.",
      "i" = "Use {.code vroom(delim =)} to explicitly specify the delimiter."
    ))
  }

  delims[[top_idx]]
}

cached <- new.env(parent = emptyenv())

vroom_threads <- function() {
  res <- as.integer(
    Sys.getenv(
      "VROOM_THREADS",
      cached$num_threads <- cached$num_threads %||% parallel::detectCores()
    )
  )
  if (is.na(res) || res <= 0) {
    res <- 1
  }
  res
}

#' Show which column types are using Altrep
#'
#' `vroom_altrep()` can be used directly as input to the `altrep`
#' argument of [vroom()].
#'
#' Alternatively there is also a family of environment variables to control use of
#' the Altrep framework. These can then be set in your `.Renviron` file, e.g.
#' with `usethis::edit_r_environ()`. The variables can take one of `true`, `false`,
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
    which$chr %||% vroom_use_altrep_chr(),
    which$fct %||% vroom_use_altrep_fct(),
    which$int %||% vroom_use_altrep_int(),
    which$dbl %||% vroom_use_altrep_dbl(),
    which$num %||% vroom_use_altrep_num(),
    which$lgl %||% vroom_use_altrep_lgl(),
    which$dttm %||% vroom_use_altrep_dttm(),
    which$date %||% vroom_use_altrep_date(),
    which$time %||% vroom_use_altrep_time(),
    which$big_int %||% vroom_use_altrep_big_int()
  )

  out <- 0L
  for (i in seq_along(args)) {
    out <- bitwOr(out, bitwShiftL(as.integer(args[[i]]), i - 1L))
  }
  structure(out, class = "vroom_altrep")
}

altrep_vals <- function() {
  c(
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
}

#' @export
print.vroom_altrep <- function(x, ...) {
  vals <- altrep_vals()
  reps <- names(vals)[bitwAnd(vals, x) > 0]

  cat(
    "Using Altrep representations for:\n",
    glue::glue(
      "
        * {reps}
       ",
      reps = glue::glue_collapse(reps, "\n * ")
    ),
    "\n",
    sep = ""
  )
}

vroom_use_altrep_chr <- function() {
  env_to_logical("VROOM_USE_ALTREP_CHR", TRUE)
}

vroom_use_altrep_fct <- function() {
  # fct is a numeric internally
  env_to_logical("VROOM_USE_ALTREP_NUMERICS", FALSE) ||
    env_to_logical("VROOM_USE_ALTREP_FCT", FALSE)
}

vroom_use_altrep_int <- function() {
  env_to_logical("VROOM_USE_ALTREP_NUMERICS", FALSE) ||
    env_to_logical("VROOM_USE_ALTREP_INT", FALSE)
}

vroom_use_altrep_big_int <- function() {
  env_to_logical("VROOM_USE_ALTREP_NUMERICS", FALSE) ||
    env_to_logical("VROOM_USE_ALTREP_BIG_INT", FALSE)
}

vroom_use_altrep_dbl <- function() {
  env_to_logical("VROOM_USE_ALTREP_NUMERICS", FALSE) ||
    env_to_logical("VROOM_USE_ALTREP_DBL", FALSE)
}

vroom_use_altrep_num <- function() {
  env_to_logical("VROOM_USE_ALTREP_NUMERICS", FALSE) ||
    env_to_logical("VROOM_USE_ALTREP_NUM", FALSE)
}

vroom_use_altrep_lgl <- function() {
  env_to_logical("VROOM_USE_ALTREP_NUMERICS", FALSE) ||
    env_to_logical("VROOM_USE_ALTREP_LGL", FALSE)
}

vroom_use_altrep_dttm <- function() {
  env_to_logical("VROOM_USE_ALTREP_NUMERICS", FALSE) ||
    env_to_logical("VROOM_USE_ALTREP_DTTM", FALSE)
}

vroom_use_altrep_date <- function() {
  env_to_logical("VROOM_USE_ALTREP_NUMERICS", FALSE) ||
    env_to_logical("VROOM_USE_ALTREP_DATE", FALSE)
}

vroom_use_altrep_time <- function() {
  env_to_logical("VROOM_USE_ALTREP_NUMERICS", FALSE) ||
    env_to_logical("VROOM_USE_ALTREP_TIME", FALSE)
}
