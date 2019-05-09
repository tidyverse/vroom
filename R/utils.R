env_to_logical <- function (var, default = TRUE) {
  res <- Sys.getenv(var, default)
  if (res %in% c("1", "yes", "true")) {
    TRUE
  } else if (res %in% c("0", "no", "false")) {
    FALSE
  } else {
    default
  }
}

is_windows <- function() {
  identical(tolower(Sys.info()[["sysname"]]), "windows")
}

`%||%` <- function(x, y) if (is.null(x)) y else x

collapse_transformer <- function(regex = "[*]$", ...) {
  function(text, envir) {
    if (grepl(regex, text)) {
      text <- sub(regex, "", text)
      res <- eval(parse(text = text, keep.source = FALSE), envir)
      glue::glue_collapse(res, ...)
    } else {
      glue::identity_transformer(text, envir)
    }
  }
}

is_named <- function (x) {
  nms <- names(x)
  if (is.null(nms)) {
    return(FALSE)
  }
  all(nms != "" & !is.na(nms))
}

deparse2 <- function(expr, ..., sep = "\n") {
  paste(deparse(expr, ...), collapse = sep)
}

is_syntactic <- function(x) make.names(x) == x

# Conditionally exported in zzz.R
# @export
compare.tbl_df <- function (x, y, ...) {
  attr(x, "spec") <- NULL
  attr(y, "spec") <- NULL
  NextMethod("compare")
}

is_rstudio_console <- function() {
  !(Sys.getenv("RSTUDIO", "") == "" || Sys.getenv("RSTUDIO_TERM", "") != "")
}

strrep <- function(x, times) {
  # This is from backports
  # https://github.com/r-lib/backports/blob/4373f8dabacd7ba288d7c559cb78146165cfdb5c/R/strrep.R#L15
  if (getRversion() < "3.3.0") {
    x = as.character(x)
    if (length(x) == 0L)
      return(x)
    unlist(.mapply(function(x, times) {
        if (is.na(x) || is.na(times))
          return(NA_character_)
        if (times <= 0L)
          return("")
        paste0(replicate(times, x), collapse = "")
}, list(x = x, times = times), MoreArgs = list()), use.names = FALSE)
  } else {
    base::strrep(x, times)
  }
}
