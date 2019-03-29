#' Read a fixed width file into a tibble
#'
#' @inheritParams vroom
#' @export
vroom_fwf <- function(file, col_positions, col_types = NULL,
                     locale = default_locale(), na = c("", "NA"),
                     comment = "", trim_ws = TRUE, skip = 0, n_max = Inf,
                     guess_max = min(n_max, 100), progress = show_progress(),
                     .name_repair = "unique") {

  file <- standardise_path(file)

  if (length(file) == 0 || (n_max == 0 & identical(col_positions$col_names, FALSE))) {
    return(tibble::tibble())
  }

  if (n_max < 0 || is.infinite(n_max)) {
    n_max <- -1
  }

  out <- vroom_fwf_(file, col_positions$begin, col_positions$end, locale = default_locale())

  tibble::as_tibble(out, .name_repair = .name_repair)
}
