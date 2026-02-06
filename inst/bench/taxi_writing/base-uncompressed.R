{
  library(vroom)
  data <- vroom(file, col_types = c(pickup_datetime = "c"))
  vroom_materialize(data, replace = FALSE)
}

write.table(
  data,
  tempfile(fileext = ".tsv"),
  sep = "\t",
  quote = FALSE,
  row.names = FALSE
)
