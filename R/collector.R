collector <- function(type, ...) {
  structure(list(...), class = c(paste0("collector_", type), "collector"))
}

is.collector <- function(x) inherits(x, "collector")

# Conditionally exported in zzz.R
# @export
print.collector <- function(x, ...) {
  cat("<", class(x)[1], ">\n", sep = "")
}
