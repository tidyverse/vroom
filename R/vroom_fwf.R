
#' @export
vroom_fwf <- function(file, pos, locale = default_locale()) {
  file <- path.expand(file)

  tibble::as_tibble(vroom_fwf_(file, pos$begin, pos$end, locale = default_locale()), .name_repair = "minimal")
}
