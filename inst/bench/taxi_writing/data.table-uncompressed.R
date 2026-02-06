{
  library(vroom)
  data <- vroom(file, col_types = c(pickup_datetime = "c"))
  vroom_materialize(data, replace = FALSE)
}

data.table::fwrite(data, tempfile(fileext = ".tsv"), sep = "\t")
