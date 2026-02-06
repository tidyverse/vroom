{
  library(vroom)
  data <- vroom(file, col_types = c(pickup_datetime = "c"))
  vroom_materialize(data, replace = FALSE)
}

readr::write_tsv(data, tempfile(fileext = ".tsv"))
