#!/usr/bin/env Rscript

library(vroom)

set.seed(42)

out_dir <- commandArgs(TRUE)[[1]]

# Simple file with all column types
data <- gen_tbl(5, col_types = "difcDtT")
vroom_write(data, file.path(out_dir, "00.tsv"))

# File with quoting and a bom
vroom_write(data, file.path(out_dir, "01.tsv"), bom = TRUE, quote = "all")


# Add a field with quotes
data[[4]][[1]] <- paste0('"f\to\no"')
vroom_write(data, file.path(out_dir, "02.tsv"), bom = TRUE, quote = "all")

# Add a header and blank lines to the start
writeLines(c("", "# foo\t, bar", vroom_format(data)), file.path(out_dir, "03.tsv"))
