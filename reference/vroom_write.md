# Write a data frame to a delimited file

Write a data frame to a delimited file

## Usage

``` r
vroom_write(
  x,
  file,
  delim = "\t",
  eol = "\n",
  na = "NA",
  col_names = !append,
  append = FALSE,
  quote = c("needed", "all", "none"),
  escape = c("double", "backslash", "none"),
  bom = FALSE,
  num_threads = vroom_threads(),
  progress = vroom_progress()
)
```

## Arguments

- x:

  A data frame or tibble to write to disk.

- file:

  File or connection to write to.

- delim:

  Delimiter used to separate values. Defaults to `\t` to write tab
  separated value (TSV) files.

- eol:

  The end of line character to use. Most commonly either `"\n"` for Unix
  style newlines, or `"\r\n"` for Windows style newlines.

- na:

  String used for missing values. Defaults to 'NA'.

- col_names:

  If `FALSE`, column names will not be included at the top of the file.
  If `TRUE`, column names will be included. If not specified,
  `col_names` will take the opposite value given to `append`.

- append:

  If `FALSE`, will overwrite existing file. If `TRUE`, will append to
  existing file. In both cases, if the file does not exist, a new file
  is created.

- quote:

  How to handle fields which contain characters that need to be quoted.

  - `needed` - Values are only quoted if needed: if they contain a
    delimiter, quote, or newline.

  - `all` - Quote all fields.

  - `none` - Never quote fields.

- escape:

  The type of escape to use when quotes are in the data.

  - `double` - quotes are escaped by doubling them.

  - `backslash` - quotes are escaped by a preceding backslash.

  - `none` - quotes are not escaped.

- bom:

  If `TRUE` add a UTF-8 BOM at the beginning of the file. This is
  recommended when saving data for consumption by excel, as it will
  force excel to read the data with the correct encoding (UTF-8)

- num_threads:

  Number of threads to use when reading and materializing vectors. If
  your data contains newlines within fields the parser will
  automatically be forced to use a single thread only.

- progress:

  Display a progress bar? By default it will only display in an
  interactive session and not while executing in an RStudio notebook
  chunk. The display of the progress bar can be disabled by setting the
  environment variable `VROOM_SHOW_PROGRESS` to `"false"`.

## Examples

``` r
# If you only specify a file name, vroom_write() will write
# the file to your current working directory.
out_file <- tempfile(fileext = "csv")
vroom_write(mtcars, out_file, ",")

# You can also use a literal filename
# vroom_write(mtcars, "mtcars.tsv")

# If you add an extension to the file name, write_()* will
# automatically compress the output.
# vroom_write(mtcars, "mtcars.tsv.gz")
# vroom_write(mtcars, "mtcars.tsv.bz2")
# vroom_write(mtcars, "mtcars.tsv.xz")
```
