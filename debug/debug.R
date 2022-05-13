devtools::clean_dll()
devtools::load_all()

vroom:::vroom_format(
  tibble::tibble(col1 = c("NATHAN", "NA", "PETER", NA)),
  delim = ",", num_threads = 1
)

