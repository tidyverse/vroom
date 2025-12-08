# Write lines to a file

Write lines to a file

## Usage

``` r
vroom_write_lines(
  x,
  file,
  eol = "\n",
  na = "NA",
  append = FALSE,
  num_threads = vroom_threads()
)
```

## Arguments

- x:

  A character vector.

- file:

  File or connection to write to.

- eol:

  The end of line character to use. Most commonly either `"\n"` for Unix
  style newlines, or `"\r\n"` for Windows style newlines.

- na:

  String used for missing values. Defaults to 'NA'.

- append:

  If `FALSE`, will overwrite existing file. If `TRUE`, will append to
  existing file. In both cases, if the file does not exist, a new file
  is created.

- num_threads:

  Number of threads to use when reading and materializing vectors. If
  your data contains newlines within fields the parser will
  automatically be forced to use a single thread only.
