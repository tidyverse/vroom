#' Create column specification
#'
#' `cols()` includes all columns in the input data, guessing the column types
#' as the default. `cols_only()` includes only the columns you explicitly
#' specify, skipping the rest.
#'
#' The available specifications are: (long names in quotes and string abbreviations in brackets)
#'
#' | function                      | long name             | short name | description                                               |
#' | ----------                    | -----------           | ---------- | -------------                                             |
#' | `col_logical()`               | "logical"             | "l"        | Logical values containing only `T`, `F`, `TRUE` or `FALSE`.              |
#' | `col_integer()`               | "integer"             | "i"        | Integer numbers.                                          |
#' | `col_big_integer()`           | "big_integer"         | "I"        | Big Integers (64bit), requires the `bit64` package.       |
#' | `col_double()`                | "double", "numeric"   | "d"        | 64-bit double floating point numbers.
#' | `col_character()`             | "character"           | "c"        | Character string data.                                              |
#' | `col_factor(levels, ordered)` | "factor"              | "f"        | A fixed set of values.                                    |
#' | `col_date(format = "")`       | "date"                | "D"        | Calendar dates formatted with the locale's `date_format`. |
#' | `col_time(format = "")`       | "time"                | "t"        | Times formatted with the locale's `time_format`.          |
#' | `col_datetime(format = "")`   | "datetime", "POSIXct" | "T"        | ISO8601 date times.                                       |
#' | `col_number()`                | "number"              | "n"        | Human readable numbers containing the `grouping_mark`     |
#' | `col_skip()`                  | "skip", "NULL"        | "_", "-"   | Skip and don't import this column.                        |
#' | `col_guess()`                 | "guess", "NA"         | "?"        | Parse using the "best" guessed type based on the input.   |
#'
#' @param ... Either column objects created by `col_*()`, or their abbreviated
#'   character names (as described in the `col_types` argument of
#'   [vroom()]). If you're only overriding a few columns, it's
#'   best to refer to columns by name. If not named, the column types must match
#'   the column names exactly. In `col_*()` functions these are stored in the
#'   object.
#' @param .default Any named columns not explicitly overridden in `...`
#'   will be read with this column type.
#' @param .delim The delimiter to use when parsing. If the `delim` argument
#'   used in the call to `vroom()` it takes precedence over the one specified in
#'   `col_types`.
#' @export
#' @aliases col_types
#' @examples
#' cols(a = col_integer())
#' cols_only(a = col_integer())
#'
#' # You can also use the standard abbreviations
#' cols(a = "i")
#' cols(a = "i", b = "d", c = "_")
#'
#' # Or long names (like utils::read.csv)
#' cols(a = "integer", b = "double", c = "skip")
#'
#' # You can also use multiple sets of column definitions by combining
#' # them like so:
#'
#' t1 <- cols(
#'   column_one = col_integer(),
#'   column_two = col_number())
#'
#' t2 <- cols(
#'  column_three = col_character())
#'
#' t3 <- t1
#' t3$cols <- c(t1$cols, t2$cols)
#' t3
cols <- function(..., .default = col_guess(), .delim = NULL) {
  col_types <- list2(...)
  is_character <- vapply(col_types, is.character, logical(1))
  col_types[is_character] <- lapply(col_types[is_character], col_concise)

  if (is.character(.default)) {
    .default <- col_concise(.default)
  }

  col_spec(col_types, .default, .delim)
}

#' @export
#' @rdname cols
cols_only <- function(...) {
  cols(..., .default = col_skip())
}


# col_spec ----------------------------------------------------------------

col_spec <- function(
  col_types,
  default = col_guess(),
  delim,
  call = caller_env()
) {
  if (!is.list(col_types)) {
    cli::cli_abort(
      "{.arg col_types} must be a column specification like that created with {.fun cols} or {.fun cols_only}, not {obj_type_friendly(col_types)}.",
      call = call
    )
  }

  hint <- "Column specifications must be created with the {.code col_*()} functions or their abbreviated character names."

  if (!is.collector(default)) {
    cli::cli_abort(
      c(
        hint,
        "x" = "Bad {.arg .default} specification."
      ),
      call = call
    )
  }

  is_collector <- vapply(col_types, is.collector, logical(1))
  if (any(!is_collector)) {
    # the as.character() is favorable for cli pluralization
    bad_idx <- as.character(which(!is_collector))
    cli::cli_abort(
      c(
        hint,
        "x" = "Bad specification{?s} at position{?s}: {bad_idx}."
      ),
      call = call
    )
  }

  structure(
    list(
      cols = col_types,
      default = default,
      delim = delim
    ),
    class = "col_spec"
  )
}

is.col_spec <- function(x) inherits(x, "col_spec")


#' Coerce to a column specification
#'
#' This is most useful for generating a specification using the short form or coercing from a list.
#'
#' @param x Input object
#' @keywords internal
#' @examples
#' as.col_spec("cccnnn")
#' @export
as.col_spec <- function(x, call = caller_env()) UseMethod("as.col_spec")

#' @export
as.col_spec.character <- function(x, call = caller_env()) {
  if (is_named(x)) {
    return(as.col_spec(as.list(x), call = call))
  }
  letters <- strsplit(x, "")[[1]]
  col_spec(lapply(letters, col_concise), col_guess(), delim = NULL, call = call)
}

#' @export
as.col_spec.NULL <- function(x, call = caller_env()) {
  col_spec(list(), delim = NULL, call = call)
}

#' @export
as.col_spec.list <- function(x, call = caller_env()) {
  do.call(cols, x)
}
#' @export
as.col_spec.col_spec <- function(x, call = caller_env()) {
  if (!"delim" %in% names(x)) {
    x["delim"] <- list(NULL)
  }
  x
}

#' @export
as.col_spec.default <- function(x, call = caller_env()) {
  cli::cli_abort(
    "{.arg col_types} must be {.code NULL}, a {.fun cols} specification, or a string, not {obj_type_friendly(x)}.",
    call = call
  )
}

# Conditionally exported in zzz.R
#' @noRd
# @export
print.col_spec <- function(
  x,
  n = Inf,
  condense = NULL,
  colour = crayon::has_color(),
  ...
) {
  cat(format.col_spec(x, n = n, condense = condense, colour = colour, ...))

  invisible(x)
}

#' @description
#' `cols_condense()` takes a spec object and condenses its definition by setting
#' the default column type to the most frequent type and only listing columns
#' with a different type.
#' @rdname spec
#' @export
cols_condense <- function(x) {
  types <- vapply(x$cols, function(xx) class(xx)[[1]], character(1))
  counts <- table(types)
  most_common <- names(counts)[counts == max(counts)][[1]]

  x$default <- x$cols[types == most_common][[1]]
  x$cols <- x$cols[types != most_common]
  x
}

# Conditionally exported in zzz.R
#' @noRd
# @export
format.col_spec <- function(
  x,
  n = Inf,
  condense = NULL,
  colour = crayon::has_color(),
  ...
) {
  if (n == 0) {
    return("")
  }

  # condense if cols >= n
  condense <- condense %||% (length(x$cols) >= n)
  if (isTRUE(condense)) {
    x <- cols_condense(x)
  }

  # truncate to minumum of n or length
  cols <- x$cols[seq_len(min(length(x$cols), n))]

  default <- NULL
  if (inherits(x$default, "collector_guess")) {
    fun_type <- "cols"
  } else if (inherits(x$default, "collector_skip")) {
    fun_type <- "cols_only"
  } else {
    fun_type <- "cols"
    type <- sub("^collector_", "", class(x$default)[[1]])
    default <- paste0(".default = col_", type, "()")
  }

  delim <- x$delim

  if (!is.null(delim) && nzchar(delim)) {
    delim <- paste0('.delim = ', glue::double_quote(delim), '')
  }

  cols_args <- c(
    default,
    vapply(
      seq_along(cols),
      function(i) {
        col_funs <- sub("^collector_", "col_", class(cols[[i]])[[1]])
        args <- vapply(cols[[i]], deparse2, character(1), sep = "\n    ")
        args <- paste(names(args), args, sep = " = ", collapse = ", ")

        col_funs <- paste0(col_funs, "(", args, ")")
        col_funs <- colourise_cols(col_funs, colour)

        col_names <- names(cols)[[i]] %||% ""

        # Need to handle unnamed columns and columns with non-syntactic names
        named <- col_names != ""

        non_syntactic <- !is_syntactic(col_names) & named
        col_names[non_syntactic] <- paste0(
          "`",
          gsub("`", "\\\\`", col_names[non_syntactic]),
          "`"
        )

        out <- paste0(col_names, " = ", col_funs)
        out[!named] <- col_funs[!named]
        out
      },
      character(1)
    ),
    delim
  )
  if (length(x$cols) == 0 && length(cols_args) == 0) {
    return(paste0(fun_type, "()\n"))
  }

  out <- paste0(fun_type, "(\n  ", paste(collapse = ",\n  ", cols_args))

  if (length(x$cols) > n) {
    out <- paste0(out, "\n  # ... with ", length(x$cols) - n, " more columns")
  }
  out <- paste0(out, "\n)\n")

  out
}

colourise_cols <- function(cols, colourise = crayon::has_color()) {
  if (!isTRUE(colourise)) {
    return(cols)
  }

  fname <- sub("[(].*", "", cols)
  for (i in seq_along(cols)) {
    cols[[i]] <- switch(
      fname,
      col_skip = ,
      col_guess = cols[[i]],

      col_character = ,
      col_factor = crayon::red(cols[[i]]),

      col_logical = crayon::yellow(cols[[i]]),

      col_double = ,
      col_integer = ,
      col_big_integer = ,
      col_number = green(cols[[i]]),

      col_date = ,
      col_datetime = ,
      col_time = blue(cols[[i]])
    )
  }
  cols
}

# This allows str() on a tibble object to print a little nicer.
# Conditionally exported in zzz.R
#' @noRd
# @export
str.col_spec <- function(object, ..., indent.str = "") {
  # Split the formatted column spec into strings
  specs <- strsplit(format(object), "\n")[[1]]
  cat(
    sep = "",
    "\n",

    # Append the current indentation string to the specs
    paste(indent.str, specs, collapse = "\n"),

    "\n"
  )
}


#' Examine the column specifications for a data frame
#'
#' `spec()` extracts the full column specification from a tibble
#' created by vroom.
#'
#' @family parsers
#' @param x The data frame object to extract from
#' @return A col_spec object.
#' @export
#' @examples
#' df <- vroom(vroom_example("mtcars.csv"))
#' s <- spec(df)
#' s
#'
#' cols_condense(s)
spec <- function(x) {
  if (!inherits(x, "tbl_df")) {
    cli::cli_abort(
      "{.arg x} must be a tibble created by vroom, not {obj_type_friendly(x)}."
    )
  }
  attr(x, "spec")
}

col_concise <- function(x, call = caller_env()) {
  switch(
    x,
    "_" = ,
    "skip" = ,
    "NULL" = ,
    "-" = col_skip(),
    "NA" = ,
    "?" = col_guess(),
    character = ,
    c = col_character(),
    factor = ,
    f = col_factor(),
    double = ,
    numeric = ,
    d = col_double(),
    integer = ,
    i = col_integer(),
    big_integer = ,
    I = col_big_integer(),
    logical = ,
    l = col_logical(),
    number = ,
    n = col_number(),
    date = ,
    Date = ,
    D = col_date(),
    datetime = ,
    POSIXct = ,
    T = col_datetime(),
    time = ,
    t = col_time(),
    cli::cli_abort(
      "Unknown column type specification: {.val {x}}",
      call = call
    )
  )
}

vroom_enquo <- function(x) {
  if (quo_is_call(x, "c") || quo_is_call(x, "list")) {
    return(as_quosures(get_expr(x)[-1], get_env(x)))
  }
  x
}

vroom_select <- function(x, col_select, id) {
  # reorder and rename columns
  if (inherits(col_select, "quosures") || !quo_is_null(col_select)) {
    if (inherits(col_select, "quosures")) {
      vars <- tidyselect::vars_select(c(names(spec(x)$cols), id), !!!col_select)
    } else {
      vars <- tidyselect::vars_select(c(names(spec(x)$cols), id), !!col_select)
    }
    if (!is.null(id) && !id %in% vars) {
      names(id) <- id
      vars <- c(id, vars)
    }
    # This can't be just names(x) as we need to have skipped
    # names as well to pass to vars_select()
    x <- x[vars]
    names(x) <- names(vars)
  }
  x
}

col_types_standardise <- function(
  spec,
  num_cols,
  col_names,
  col_select,
  name_repair
) {
  if (num_cols == 0) {
    if (length(spec$cols) > 0) {
      num_cols <- length(spec$cols)
    } else if (length(col_names) > 0) {
      num_cols <- length(col_names)
    }
  }

  if (length(col_names) == 0) {
    col_names <- make_names(NULL, num_cols)
  }

  col_names <- vctrs::vec_as_names(col_names, repair = name_repair)

  type_names <- names(spec$cols)

  if (length(spec$cols) == 0) {
    # no types specified so use defaults

    spec$cols <- rep(list(spec$default), num_cols)
    names(spec$cols) <- col_names[seq_along(spec$cols)]
  } else if (is.null(type_names)) {
    # unnamed types & names guessed from header: match exactly
    if (num_cols < length(spec$cols)) {
      spec$cols <- spec$cols[seq_len(num_cols)]
    } else {
      spec$cols <- c(
        spec$cols,
        rep(list(spec$default), num_cols - length(spec$cols))
      )
    }
    names(spec$cols) <- col_names[seq_along(spec$cols)]
  } else {
    # named types

    if (num_cols > length(col_names)) {
      col_names <- make_names(col_names, num_cols)
    }

    bad_types <- !(type_names %in% col_names)
    if (any(bad_types)) {
      warn(
        paste0(
          "The following named parsers don't match the column names: ",
          paste0(type_names[bad_types], collapse = ", ")
        ),
        class = "vroom_mismatched_column_name"
      )
      spec$cols <- spec$cols[!bad_types]
      type_names <- type_names[!bad_types]
    }

    default_types <- !(col_names %in% type_names)
    if (any(default_types)) {
      defaults <- rep(list(spec$default), sum(default_types))
      names(defaults) <- col_names[default_types]
      spec$cols[names(defaults)] <- defaults
    }

    spec$cols <- spec$cols[col_names]
  }

  if (inherits(col_select, "quosures") || !quo_is_null(col_select)) {
    if (inherits(col_select, "quosures")) {
      to_keep <- names(spec$cols) %in%
        tidyselect::vars_select(
          names(spec$cols),
          !!!col_select,
          .strict = FALSE
        )
    } else {
      to_keep <- names(spec$cols) %in%
        tidyselect::vars_select(names(spec$cols), !!col_select, .strict = FALSE)
    }

    spec$cols[!to_keep] <- rep(list(col_skip()), sum(!to_keep))
  }

  # Set the names, ignoring skipped columns
  kept <- !vapply(spec$cols, inherits, logical(1), "collector_skip")

  # Fill the column names if they are shorter than what is kept.
  if (length(col_names) == length(spec$cols)) {
    names(spec$cols)[kept] <- col_names[kept]
  } else if (length(col_names) == sum(kept)) {
    names(spec$cols)[kept] <- col_names
  } else {
    col_names <- make_names(col_names, sum(kept))
    names(spec$cols)[kept] <- col_names
  }

  spec
}


#' Guess the type of a vector
#'
#' @param x Character vector of values to parse.
#' @inheritParams vroom
#' @param guess_integer If `TRUE`, guess integer types for whole numbers, if
#'   `FALSE` guess numeric type for all numbers.
#' @examples
#'  # Logical vectors
#'  guess_type(c("FALSE", "TRUE", "F", "T"))

#'  # Integers and doubles
#'  guess_type(c("1","2","3"))
#'  guess_type(c("1.6","2.6","3.4"))

#'  # Numbers containing grouping mark
#'  guess_type("1,234,566")

#'  # ISO 8601 date times
#'  guess_type(c("2010-10-10"))
#'  guess_type(c("2010-10-10 01:02:03"))
#'  guess_type(c("01:02:03 AM"))
#' @export
guess_type <- function(
  x,
  na = c("", "NA"),
  locale = default_locale(),
  guess_integer = FALSE
) {
  type <- guess_type_(
    x,
    na = na,
    locale = locale,
    guess_integer = guess_integer
  )
  get(paste0("col_", type), asNamespace("vroom"))()
}

guess_parser <- function(
  x,
  na = c("", "NA"),
  locale = default_locale(),
  guess_integer = FALSE
) {
  guess_type_(x, na = na, locale = locale, guess_integer = guess_integer)
}

show_dims <- function(x) {
  cli_block(class = "vroom_dim_message", {
    cli::cli_text(
      "
      {.strong Rows: }{.val {NROW(x)}}
      {.strong Columns: }{.val {NCOL(x)}}
      "
    )
  })
}

collector_value <- function(x, ...) {
  UseMethod("collector_value")
}

#' @export
collector_value.collector_character <- function(x, ...) {
  character()
}

#' @export
collector_value.collector_double <- function(x, ...) {
  numeric()
}

#' @export
collector_value.collector_integer <- function(x, ...) {
  integer()
}

#' @export
collector_value.collector_number <- function(x, ...) {
  numeric()
}

#' @export
collector_value.collector_logical <- function(x, ...) {
  logical()
}

#' @export
collector_value.collector_factor <- function(x, ...) {
  factor()
}

#' @export
collector_value.collector_datetime <- function(x, ...) {
  vctrs::new_datetime()
}

#' @export
collector_value.collector_date <- function(x, ...) {
  vctrs::new_date()
}

#' @export
collector_value.collector_time <- function(x, ...) {
  hms::hms()
}

#' @export
collector_value.collector_guess <- function(x, ...) {
  character()
}

#' @export
collector_value.collector_skip <- function(x, ...) {
  NULL
}

#' @export
summary.col_spec <- function(
  object,
  width = getOption("width"),
  locale = default_locale(),
  ...
) {
  if (length(object$cols) == 0) {
    return(invisible(object))
  }

  type_map <- c(
    "collector_character" = "chr",
    "collector_double" = "dbl",
    "collector_integer" = "int",
    "collector_number" = "num",
    "collector_logical" = "lgl",
    "collector_factor" = "fct",
    "collector_datetime" = "dttm",
    "collector_date" = "date",
    "collector_time" = "time",
    "collector_guess" = "???"
  )

  col_types <- vapply(object$cols, function(x) class(x)[[1]], character(1))
  col_types <- droplevels(factor(
    type_map[col_types],
    levels = unname(type_map)
  ))
  type_counts <- table(col_types)

  n <- length(type_counts)

  types <- format(vapply(names(type_counts), color_type, character(1)))
  counts <- format(glue::glue("({type_counts})"), justify = "right")
  col_width <- min(width - (crayon::col_nchar(types) + nchar(counts) + 4))
  columns <- vapply(
    split(names(object$cols), col_types),
    function(x) glue::glue_collapse(x, ", ", width = col_width),
    character(1)
  )

  fmt_num <- function(x) {
    prettyNum(
      x,
      big.mark = locale$grouping_mark,
      decimal.mark = locale$decimal_mark
    )
  }

  delim <- object$delim %||% ""

  txt <- glue::glue(
    .transformer = collapse_transformer(sep = "\n"),
    entries = glue::glue("{format(types)} {counts}: {columns}"),

    '
    {if (nzchar(delim)) paste(bold("Delimiter:"), glue::double_quote(delim)) else ""}
    {entries*}


    '
  )
  cli_block(class = "vroom_spec_message", {
    cli::cli_h1("Column specification")
    cli::cli_verbatim(txt)
  })

  invisible(object)
}

show_col_types <- function(x, locale) {
  show_dims(x)
  summary(spec(x), locale = locale)
  cli_block(class = "vroom_spec_message", {
    cli::cli_verbatim("\n\n")
    cli::cli_alert_info(
      "Use {.fn spec} to retrieve the full column specification for this data."
    )
    cli::cli_alert_info(
      "Specify the column types or set {.arg show_col_types = FALSE} to quiet this message."
    )
  })
}

cli_block <- function(expr, class = NULL, type = rlang::inform) {
  msg <- ""
  withCallingHandlers(
    expr,
    message = function(x) {
      msg <<- paste0(msg, x$message)
      invokeRestart("muffleMessage")
    }
  )
  msg <- sub("^\n", "", msg)
  msg <- sub("\n+$", "", msg)

  type(msg, class = class)
}

color_type <- function(type) {
  switch(
    type,
    chr = ,
    fct = crayon::red(type),
    lgl = crayon::yellow(type),
    dbl = ,
    int = ,
    num = green(type),
    date = ,
    dttm = ,
    time = blue(type),
    "???" = type
  )
}

#' @rdname cols
#' @export
col_logical <- function(...) {
  collector("logical", ...)
}

#' @rdname cols
#' @export
col_integer <- function(...) {
  collector("integer", ...)
}

#' @rdname cols
#' @export
col_big_integer <- function(...) {
  collector("big_integer", ...)
}

#' @rdname cols
#' @export
col_double <- function(...) {
  collector("double", ...)
}

#' @rdname cols
#' @export
col_character <- function(...) {
  collector("character", ...)
}

#' @rdname cols
#' @export
col_skip <- function(...) {
  collector("skip", ...)
}

#' @rdname cols
#' @export
col_number <- function(...) {
  collector("number", ...)
}

#' @rdname cols
#' @export
col_guess <- function(...) {
  collector("guess", ...)
}

#' @rdname cols
#' @param levels Character vector of the allowed levels. When `levels = NULL`
#'   (the default), `levels` are discovered from the unique values of the data,
#'   in the order in which they are encountered.
#' @param ordered Is it an ordered factor?
#' @param include_na If `TRUE` and the data contains at least one `NA`, then
#'   `NA` is included in the levels of the constructed factor.
#' @export
col_factor <- function(
  levels = NULL,
  ordered = FALSE,
  include_na = FALSE,
  ...
) {
  collector(
    "factor",
    levels = levels,
    ordered = ordered,
    include_na = include_na,
    ...
  )
}

#' @rdname cols
#' @param format A format specification. If set to "":
#'   * `col_datetime()` expects ISO8601 datetimes. Here are some examples of
#'     input that should just work:
#'     "2024-01-15", "2024-01-15 14:30:00", "2024-01-15T14:30:00Z".
#'   * `col_date()` uses the `date_format` from [locale()] (default `"%AD"`).
#'     These inputs should just work: "2024-01-15", "01/15/2024".
#'   * `col_time()` uses the `time_format` from [locale()] (default `"%AT"`).
#'     These inputs should just work: "14:30:00", "2:30:00 PM".
#'
#'   Unlike [strptime()], the format specification must match the complete
#'   string. For more details, see below.
#'
#' @details
#' ## Date, time, and datetime formats:
#' \pkg{vroom} uses a format specification similar to [strptime()].
#' There are three types of element:
#'
#' 1. A conversion specification that is "%" followed by a letter. For example
#'   "%Y" matches a 4 digit year, "%m", matches a 2 digit month and "%d" matches
#'   a 2 digit day. Month and day default to `1`, (i.e. Jan 1st) if not present,
#'   for example if only a year is given.
#' 2. Whitespace is any sequence of zero or more whitespace characters.
#' 3. Any other character is matched exactly.
#'
#' \pkg{vroom}'s datetime `col_*()` functions recognize the following
#' specifications:
#'
#' * Year: "%Y" (4 digits). "%y" (2 digits); 00-69 -> 2000-2069, 70-99 ->
#'   1970-1999.
#' * Month: "%m" (2 digits), "%b" (abbreviated name in current locale), "%B"
#'   (full name in current locale).
#' * Day: "%d" (2 digits), "%e" (optional leading space), "%a" (abbreviated
#'   name in current locale).
#' * Hour: "%H" or "%I" or "%h", use I (and not H) with AM/PM, use h (and not H)
#'   if your times represent durations longer than one day.
#' * Minutes: "%M"
#' * Seconds: "%S" (integer seconds), "%OS" (partial seconds)
#' * Time zone: "%Z" (as name, e.g. "America/Chicago"), "%z" (as offset from
#'   UTC, e.g. "+0800")
#' * AM/PM indicator: "%p".
#' * Non-digits: "%." skips one non-digit character, "%+" skips one or more
#'   non-digit characters, "%*" skips any number of non-digits characters.
#' * Automatic parsers: "%AD" parses with a flexible YMD parser, "%AT" parses
#'   with a flexible HMS parser.
#' * Shortcuts: "%D" = "%m/%d/%y", "%F" = "%Y-%m-%d", "%R" = "%H:%M", "%T" =
#'   "%H:%M:%S", "%x" = "%y/%m/%d".
#'
#' ### ISO8601 support
#'
#' Currently, vroom does not support all of ISO8601. Missing features:
#'
#' * Week & weekday specifications, e.g. "2013-W05", "2013-W05-10".
#' * Ordinal dates, e.g. "2013-095".
#' * Using commas instead of a period for decimal separator.
#'
#' The parser is also a little laxer than ISO8601:
#'
#' * Dates and times can be separated with a space, not just T.
#' * Mostly correct specifications like "2009-05-19 14:" and "200912-01" work.
#'
#' @export
col_datetime <- function(format = "", ...) {
  collector("datetime", format = format, ...)
}

#' @rdname cols
#' @export
col_date <- function(format = "", ...) {
  collector("date", format = format, ...)
}

#' @rdname cols
#' @export
col_time <- function(format = "", ...) {
  collector("time", format = format, ...)
}
