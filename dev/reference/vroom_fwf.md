# Read a fixed-width file into a tibble

Fixed-width files store tabular data with each field occupying a
specific range of character positions in every line. Once the fields are
identified, converting them to the appropriate R types works just like
for delimited files. The unique challenge with fixed-width files is
describing where each field begins and ends. vroom tries to ease this
pain by offering a few different ways to specify the field structure:

- `fwf_empty()` - Guesses based on the positions of empty columns. This
  is the default. (Note that `fwf_empty()` returns 0-based positions,
  for internal use.)

- `fwf_widths()` - Supply the widths of the columns.

- `fwf_positions()` - Supply paired vectors of start and end positions.
  These are interpreted as 1-based positions, so are off-by-one compared
  to the output of `fwf_empty()`.

- `fwf_cols()` - Supply named arguments of paired start and end
  positions or column widths.

Note: `fwf_empty()` cannot work with a connection or with any of the
input types that involve a connection internally, which includes remote
and compressed files. The reason is that this would necessitate reading
from the connection twice. In these cases, you'll have to either provide
the field structure explicitly with another `fwf_*()` function or
download (and decompress, if relevant) the file first.

## Usage

``` r
vroom_fwf(
  file,
  col_positions = fwf_empty(file, skip, n = guess_max),
  col_types = NULL,
  col_select = NULL,
  id = NULL,
  locale = default_locale(),
  na = c("", "NA"),
  comment = "",
  skip_empty_rows = TRUE,
  trim_ws = TRUE,
  skip = 0,
  n_max = Inf,
  guess_max = 100,
  altrep = TRUE,
  num_threads = vroom_threads(),
  progress = vroom_progress(),
  show_col_types = NULL,
  .name_repair = "unique"
)

fwf_empty(file, skip = 0, col_names = NULL, comment = "", n = 100L)

fwf_widths(widths, col_names = NULL)

fwf_positions(start, end = NULL, col_names = NULL)

fwf_cols(...)
```

## Arguments

- file:

  Either a path to a file, a connection, or literal data (either a
  single string or a raw vector). `file` can also be a character vector
  containing multiple filepaths or a list containing multiple
  connections.

  Files ending in `.gz`, `.bz2`, `.xz`, or `.zip` will be automatically
  uncompressed. Files starting with `http://`, `https://`, `ftp://`, or
  `ftps://` will be automatically downloaded. Remote `.gz` files can
  also be automatically downloaded and decompressed.

  Literal data is most useful for examples and tests. To be recognised
  as literal data, wrap the input with
  [`I()`](https://rdrr.io/r/base/AsIs.html).

- col_positions:

  Column positions, as created by `fwf_empty()`, `fwf_widths()`,
  `fwf_positions()`, or `fwf_cols()`. To read in only selected fields,
  use `fwf_positions()`. If the width of the last column is variable (a
  ragged fwf file), supply the last end position as `NA`.

- col_types:

  One of `NULL`, a
  [`cols()`](https://vroom.tidyverse.org/dev/reference/cols.md)
  specification, or a string.

  If `NULL`, all column types will be inferred from `guess_max` rows of
  the input, interspersed throughout the file. This is convenient (and
  fast), but not robust. If the guessed types are wrong, you'll need to
  increase `guess_max` or supply the correct types yourself.

  Column specifications created by
  [`list()`](https://rdrr.io/r/base/list.html) or
  [`cols()`](https://vroom.tidyverse.org/dev/reference/cols.md) must
  contain one column specification for each column. If you only want to
  read a subset of the columns, use
  [`cols_only()`](https://vroom.tidyverse.org/dev/reference/cols.md).

  Alternatively, you can use a compact string representation where each
  character represents one column:

  - c = character

  - i = integer

  - n = number

  - d = double

  - l = logical

  - f = factor

  - D = date

  - T = date time

  - t = time

  - ? = guess

  - \_ or - = skip

  By default, reading a file without a column specification will print a
  message showing the guessed types. To suppress this message, set
  `show_col_types = FALSE`.

- col_select:

  Columns to include in the results. You can use the same mini-language
  as
  [`dplyr::select()`](https://dplyr.tidyverse.org/reference/select.html)
  to refer to the columns by name. Use
  [`c()`](https://rdrr.io/r/base/c.html) to use more than one selection
  expression. Although this usage is less common, `col_select` also
  accepts a numeric column index. See
  [`?tidyselect::language`](https://tidyselect.r-lib.org/reference/language.html)
  for full details on the selection language.

- id:

  Either a string or 'NULL'. If a string, the output will contain a
  column with that name with the filename(s) as the value, i.e. this
  column effectively tells you the source of each row. If 'NULL' (the
  default), no such column will be created.

- locale:

  The locale controls defaults that vary from place to place. The
  default locale is US-centric (like R), but you can use
  [`locale()`](https://vroom.tidyverse.org/dev/reference/locale.md) to
  create your own locale that controls things like the default time
  zone, encoding, decimal mark, big mark, and day/month names.

- na:

  Character vector of strings to interpret as missing values. Set this
  option to [`character()`](https://rdrr.io/r/base/character.html) to
  indicate no missing values.

- comment:

  A string used to identify comments. Any line that starts with the
  comment string at the beginning of the file (before any data lines)
  will be ignored. Unlike
  [`vroom()`](https://vroom.tidyverse.org/dev/reference/vroom.md),
  comment lines in the middle of the file are not filtered out.

- skip_empty_rows:

  Should blank rows be ignored altogether? i.e. If this option is `TRUE`
  then blank rows will not be represented at all. If it is `FALSE` then
  they will be represented by `NA` values in all the columns.

- trim_ws:

  Should leading and trailing whitespace (ASCII spaces and tabs) be
  trimmed from each field before parsing it?

- skip:

  Number of lines to skip before reading data. If `comment` is supplied
  any commented lines are ignored *after* skipping.

- n_max:

  Maximum number of lines to read.

- guess_max:

  Maximum number of lines to use for guessing column types. See
  [`vignette("column-types", package = "readr")`](https://readr.tidyverse.org/articles/column-types.html)
  for more details.

- altrep:

  Control which column types use Altrep representations, either a
  character vector of types, `TRUE` or `FALSE`. See
  [`vroom_altrep()`](https://vroom.tidyverse.org/dev/reference/vroom_altrep.md)
  for for full details.

- num_threads:

  Number of threads to use when reading and materializing vectors. If
  your data contains newlines within fields the parser will
  automatically be forced to use a single thread only.

- progress:

  Display a progress bar? By default it will only display in an
  interactive session and not while executing in an RStudio notebook
  chunk. The display of the progress bar can be disabled by setting the
  environment variable `VROOM_SHOW_PROGRESS` to `"false"`.

- show_col_types:

  Control showing the column specifications. If `TRUE` column
  specifications are always shown, if `FALSE` they are never shown. If
  `NULL` (the default), they are shown only if an explicit specification
  is not given in `col_types`, i.e. if the types have been guessed.

- .name_repair:

  Handling of column names. The default behaviour is to ensure column
  names are `"unique"`. Various repair strategies are supported:

  - `"minimal"`: No name repair or checks, beyond basic existence of
    names.

  - `"unique"` (default value): Make sure names are unique and not
    empty.

  - `"check_unique"`: No name repair, but check they are `unique`.

  - `"unique_quiet"`: Repair with the `unique` strategy, quietly.

  - `"universal"`: Make the names `unique` and syntactic.

  - `"universal_quiet"`: Repair with the `universal` strategy, quietly.

  - A function: Apply custom name repair (e.g.,
    `name_repair = make.names` for names in the style of base R).

  - A purrr-style anonymous function, see
    [`rlang::as_function()`](https://rlang.r-lib.org/reference/as_function.html).

  This argument is passed on as `repair` to
  [`vctrs::vec_as_names()`](https://vctrs.r-lib.org/reference/vec_as_names.html).
  See there for more details on these terms and the strategies used to
  enforce them.

- col_names:

  Either NULL, or a character vector column names.

- n:

  Number of lines the tokenizer will read to determine file structure.
  By default it is set to 100.

- widths:

  Width of each field. Use `NA` as the width of the last field when
  reading a ragged fixed-width file.

- start, end:

  Starting and ending (inclusive) positions of each field. **Positions
  are 1-based**: the first character in a line is at position 1. Use
  `NA` as the last value of `end` when reading a ragged fixed-width
  file.

- ...:

  Named or unnamed arguments, each addressing one column. Each input
  should be either a single integer (a column width) or a pair of
  integers (column start and end positions). All arguments must have the
  same shape, i.e. all widths or all positions.

## Details

Here's a enhanced example using the contents of the file accessed via
`vroom_example("fwf-sample.txt")`.

             1         2         3         4
    123456789012345678901234567890123456789012
    [     name 20      ][state 10][  ssn 12  ]
    John Smith          WA        418-Y11-4111
    Mary Hartford       CA        319-Z19-4341
    Evan Nolan          IL        219-532-c301

Here are some valid field specifications for the above (they aren't all
equivalent! but they are all valid):

    fwf_widths(c(20, 10, 12), c("name", "state", "ssn"))
    fwf_positions(c(1, 30), c(20, 42), c("name", "ssn"))
    fwf_cols(state = c(21, 30), last = c(6, 20), first = c(1, 4), ssn = c(31, 42))
    fwf_cols(name = c(1, 20), ssn = c(30, 42))
    fwf_cols(name = 20, state = 10, ssn = 12)

## Examples

``` r
fwf_sample <- vroom_example("fwf-sample.txt")
writeLines(vroom_lines(fwf_sample))
#> John Smith          WA        418-Y11-4111
#> Mary Hartford       CA        319-Z19-4341
#> Evan Nolan          IL        219-532-c301

# You can specify column positions in several ways:
# 1. Guess based on position of empty columns
vroom_fwf(fwf_sample, fwf_empty(fwf_sample, col_names = c("first", "last", "state", "ssn")))
#> Rows: 3 Columns: 4
#> ── Column specification ───────────────────────────────────────────────
#> 
#> chr (4): first, last, state, ssn
#> 
#> ℹ Use `spec()` to retrieve the full column specification for this data.
#> ℹ Specify the column types or set `show_col_types = FALSE` to quiet this message.
#> # A tibble: 3 × 4
#>   first last     state ssn         
#>   <chr> <chr>    <chr> <chr>       
#> 1 John  Smith    WA    418-Y11-4111
#> 2 Mary  Hartford CA    319-Z19-4341
#> 3 Evan  Nolan    IL    219-532-c301
# 2. A vector of field widths
vroom_fwf(fwf_sample, fwf_widths(c(20, 10, 12), c("name", "state", "ssn")))
#> Rows: 3 Columns: 3
#> ── Column specification ───────────────────────────────────────────────
#> 
#> chr (3): name, state, ssn
#> 
#> ℹ Use `spec()` to retrieve the full column specification for this data.
#> ℹ Specify the column types or set `show_col_types = FALSE` to quiet this message.
#> # A tibble: 3 × 3
#>   name          state ssn         
#>   <chr>         <chr> <chr>       
#> 1 John Smith    WA    418-Y11-4111
#> 2 Mary Hartford CA    319-Z19-4341
#> 3 Evan Nolan    IL    219-532-c301
# 3. Paired vectors of start and end positions
vroom_fwf(fwf_sample, fwf_positions(c(1, 30), c(20, 42), c("name", "ssn")))
#> Rows: 3 Columns: 2
#> ── Column specification ───────────────────────────────────────────────
#> 
#> chr (2): name, ssn
#> 
#> ℹ Use `spec()` to retrieve the full column specification for this data.
#> ℹ Specify the column types or set `show_col_types = FALSE` to quiet this message.
#> # A tibble: 3 × 2
#>   name          ssn         
#>   <chr>         <chr>       
#> 1 John Smith    418-Y11-4111
#> 2 Mary Hartford 319-Z19-4341
#> 3 Evan Nolan    219-532-c301
# 4. Named arguments with start and end positions
vroom_fwf(fwf_sample, fwf_cols(name = c(1, 20), ssn = c(30, 42)))
#> Rows: 3 Columns: 2
#> ── Column specification ───────────────────────────────────────────────
#> 
#> chr (2): name, ssn
#> 
#> ℹ Use `spec()` to retrieve the full column specification for this data.
#> ℹ Specify the column types or set `show_col_types = FALSE` to quiet this message.
#> # A tibble: 3 × 2
#>   name          ssn         
#>   <chr>         <chr>       
#> 1 John Smith    418-Y11-4111
#> 2 Mary Hartford 319-Z19-4341
#> 3 Evan Nolan    219-532-c301
# 5. Named arguments with column widths
vroom_fwf(fwf_sample, fwf_cols(name = 20, state = 10, ssn = 12))
#> Rows: 3 Columns: 3
#> ── Column specification ───────────────────────────────────────────────
#> 
#> chr (3): name, state, ssn
#> 
#> ℹ Use `spec()` to retrieve the full column specification for this data.
#> ℹ Specify the column types or set `show_col_types = FALSE` to quiet this message.
#> # A tibble: 3 × 3
#>   name          state ssn         
#>   <chr>         <chr> <chr>       
#> 1 John Smith    WA    418-Y11-4111
#> 2 Mary Hartford CA    319-Z19-4341
#> 3 Evan Nolan    IL    219-532-c301
```
