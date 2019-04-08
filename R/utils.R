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
