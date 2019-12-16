{
  library(vroom)
  data <- vroom(file, col_types = c(pickup_datetime = "c"))
  vroom:::vroom_materialize(data)
}

vroom_write(data, pipe(sprintf("pigz > %s", tempfile(fileext = ".gz"))), delim = "\t")
