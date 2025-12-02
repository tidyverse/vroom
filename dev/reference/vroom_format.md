# Convert a data frame to a delimited string

This is equivalent to
[`vroom_write()`](https://vroom.tidyverse.org/dev/reference/vroom_write.md),
but instead of writing to disk, it returns a string. It is primarily
useful for examples and for testing.

## Usage

``` r
vroom_format(
  x,
  delim = "\t",
  eol = "\n",
  na = "NA",
  col_names = TRUE,
  escape = c("double", "backslash", "none"),
  quote = c("needed", "all", "none"),
  bom = FALSE,
  num_threads = vroom_threads()
)
```

## Arguments

- x:

  A data frame or tibble to write to disk.

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

- escape:

  The type of escape to use when quotes are in the data.

  - `double` - quotes are escaped by doubling them.

  - `backslash` - quotes are escaped by a preceding backslash.

  - `none` - quotes are not escaped.

- quote:

  How to handle fields which contain characters that need to be quoted.

  - `needed` - Values are only quoted if needed: if they contain a
    delimiter, quote, or newline.

  - `all` - Quote all fields.

  - `none` - Never quote fields.

- bom:

  If `TRUE` add a UTF-8 BOM at the beginning of the file. This is
  recommended when saving data for consumption by excel, as it will
  force excel to read the data with the correct encoding (UTF-8)

- num_threads:

  Number of threads to use when reading and materializing vectors. If
  your data contains newlines within fields the parser will
  automatically be forced to use a single thread only.
