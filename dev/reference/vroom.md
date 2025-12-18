# Read a delimited file into a tibble

Read a delimited file into a tibble

## Usage

``` r
vroom(
  file,
  delim = NULL,
  col_names = TRUE,
  col_types = NULL,
  col_select = NULL,
  id = NULL,
  skip = 0,
  n_max = Inf,
  na = c("", "NA"),
  quote = "\"",
  comment = "",
  skip_empty_rows = TRUE,
  trim_ws = TRUE,
  escape_double = TRUE,
  escape_backslash = FALSE,
  locale = default_locale(),
  guess_max = 100,
  altrep = TRUE,
  num_threads = vroom_threads(),
  progress = vroom_progress(),
  show_col_types = NULL,
  .name_repair = "unique"
)
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

- delim:

  One or more characters used to delimit fields within a file. If `NULL`
  the delimiter is guessed from the set of
  `c(",", "\t", " ", "|", ":", ";")`.

- col_names:

  Either `TRUE`, `FALSE` or a character vector of column names.

  If `TRUE`, the first row of the input will be used as the column
  names, and will not be included in the data frame. If `FALSE`, column
  names will be generated automatically: X1, X2, X3 etc.

  If `col_names` is a character vector, the values will be used as the
  names of the columns, and the first row of the input will be read into
  the first row of the output data frame.

  Missing (`NA`) column names will generate a warning, and be filled in
  with dummy names `...1`, `...2` etc. Duplicate column names will
  generate a warning and be made unique, see `name_repair` to control
  how this is done.

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

- skip:

  Number of lines to skip before reading data. If `comment` is supplied
  any commented lines are ignored *after* skipping.

- n_max:

  Maximum number of lines to read.

- na:

  Character vector of strings to interpret as missing values. Set this
  option to [`character()`](https://rdrr.io/r/base/character.html) to
  indicate no missing values.

- quote:

  Single character used to quote strings.

- comment:

  A string used to identify comments. Any text after the comment
  characters will be silently ignored.

- skip_empty_rows:

  Should blank rows be ignored altogether? i.e. If this option is `TRUE`
  then blank rows will not be represented at all. If it is `FALSE` then
  they will be represented by `NA` values in all the columns.

- trim_ws:

  Should leading and trailing whitespace (ASCII spaces and tabs) be
  trimmed from each field before parsing it?

- escape_double:

  Does the file escape quotes by doubling them? i.e. If this option is
  `TRUE`, the value '""' represents a single quote, '"'.

- escape_backslash:

  Does the file use backslashes to escape special characters? This is
  more general than `escape_double` as backslashes can be used to escape
  the delimiter character, the quote character, or to add special
  characters like `\\n`.

- locale:

  The locale controls defaults that vary from place to place. The
  default locale is US-centric (like R), but you can use
  [`locale()`](https://vroom.tidyverse.org/dev/reference/locale.md) to
  create your own locale that controls things like the default time
  zone, encoding, decimal mark, big mark, and day/month names.

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

## Examples

``` r
# get path to example file
input_file <- vroom_example("mtcars.csv")
input_file
#> [1] "/home/runner/work/_temp/Library/vroom/extdata/mtcars.csv"

# Read from a path

# Input sources -------------------------------------------------------------
# Read from a path
vroom(input_file)
#> Rows: 32 Columns: 12
#> ── Column specification ───────────────────────────────────────────────
#> Delimiter: ","
#> chr  (1): model
#> dbl (11): mpg, cyl, disp, hp, drat, wt, qsec, vs, am, gear, carb
#> 
#> ℹ Use `spec()` to retrieve the full column specification for this data.
#> ℹ Specify the column types or set `show_col_types = FALSE` to quiet this message.
#> # A tibble: 32 × 12
#>    model      mpg   cyl  disp    hp  drat    wt  qsec    vs    am  gear
#>    <chr>    <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl>
#>  1 Mazda R…  21       6  160    110  3.9   2.62  16.5     0     1     4
#>  2 Mazda R…  21       6  160    110  3.9   2.88  17.0     0     1     4
#>  3 Datsun …  22.8     4  108     93  3.85  2.32  18.6     1     1     4
#>  4 Hornet …  21.4     6  258    110  3.08  3.22  19.4     1     0     3
#>  5 Hornet …  18.7     8  360    175  3.15  3.44  17.0     0     0     3
#>  6 Valiant   18.1     6  225    105  2.76  3.46  20.2     1     0     3
#>  7 Duster …  14.3     8  360    245  3.21  3.57  15.8     0     0     3
#>  8 Merc 24…  24.4     4  147.    62  3.69  3.19  20       1     0     4
#>  9 Merc 230  22.8     4  141.    95  3.92  3.15  22.9     1     0     4
#> 10 Merc 280  19.2     6  168.   123  3.92  3.44  18.3     1     0     4
#> # ℹ 22 more rows
#> # ℹ 1 more variable: carb <dbl>
# You can also use paths directly
# vroom("mtcars.csv")

if (FALSE) { # \dontrun{
# Including remote paths
vroom("https://github.com/tidyverse/vroom/raw/main/inst/extdata/mtcars.csv")
} # }

# Or directly from a string with `I()`
vroom(I("x,y\n1,2\n3,4\n"))
#> Rows: 2 Columns: 2
#> ── Column specification ───────────────────────────────────────────────
#> Delimiter: ","
#> dbl (2): x, y
#> 
#> ℹ Use `spec()` to retrieve the full column specification for this data.
#> ℹ Specify the column types or set `show_col_types = FALSE` to quiet this message.
#> # A tibble: 2 × 2
#>       x     y
#>   <dbl> <dbl>
#> 1     1     2
#> 2     3     4

# Column selection ----------------------------------------------------------
# Pass column names or indexes directly to select them
vroom(input_file, col_select = c(model, cyl, gear))
#> Rows: 32 Columns: 3
#> ── Column specification ───────────────────────────────────────────────
#> Delimiter: ","
#> chr (1): model
#> dbl (2): cyl, gear
#> 
#> ℹ Use `spec()` to retrieve the full column specification for this data.
#> ℹ Specify the column types or set `show_col_types = FALSE` to quiet this message.
#> # A tibble: 32 × 3
#>    model               cyl  gear
#>    <chr>             <dbl> <dbl>
#>  1 Mazda RX4             6     4
#>  2 Mazda RX4 Wag         6     4
#>  3 Datsun 710            4     4
#>  4 Hornet 4 Drive        6     3
#>  5 Hornet Sportabout     8     3
#>  6 Valiant               6     3
#>  7 Duster 360            8     3
#>  8 Merc 240D             4     4
#>  9 Merc 230              4     4
#> 10 Merc 280              6     4
#> # ℹ 22 more rows
vroom(input_file, col_select = c(1, 3, 11))
#> Rows: 32 Columns: 3
#> ── Column specification ───────────────────────────────────────────────
#> Delimiter: ","
#> chr (1): model
#> dbl (2): cyl, gear
#> 
#> ℹ Use `spec()` to retrieve the full column specification for this data.
#> ℹ Specify the column types or set `show_col_types = FALSE` to quiet this message.
#> # A tibble: 32 × 3
#>    model               cyl  gear
#>    <chr>             <dbl> <dbl>
#>  1 Mazda RX4             6     4
#>  2 Mazda RX4 Wag         6     4
#>  3 Datsun 710            4     4
#>  4 Hornet 4 Drive        6     3
#>  5 Hornet Sportabout     8     3
#>  6 Valiant               6     3
#>  7 Duster 360            8     3
#>  8 Merc 240D             4     4
#>  9 Merc 230              4     4
#> 10 Merc 280              6     4
#> # ℹ 22 more rows

# Or use the selection helpers
vroom(input_file, col_select = starts_with("d"))
#> Rows: 32 Columns: 2
#> ── Column specification ───────────────────────────────────────────────
#> Delimiter: ","
#> dbl (2): disp, drat
#> 
#> ℹ Use `spec()` to retrieve the full column specification for this data.
#> ℹ Specify the column types or set `show_col_types = FALSE` to quiet this message.
#> # A tibble: 32 × 2
#>     disp  drat
#>    <dbl> <dbl>
#>  1  160   3.9 
#>  2  160   3.9 
#>  3  108   3.85
#>  4  258   3.08
#>  5  360   3.15
#>  6  225   2.76
#>  7  360   3.21
#>  8  147.  3.69
#>  9  141.  3.92
#> 10  168.  3.92
#> # ℹ 22 more rows

# You can also rename specific columns
vroom(input_file, col_select = c(car = model, everything()))
#> Rows: 32 Columns: 12
#> ── Column specification ───────────────────────────────────────────────
#> Delimiter: ","
#> chr  (1): model
#> dbl (11): mpg, cyl, disp, hp, drat, wt, qsec, vs, am, gear, carb
#> 
#> ℹ Use `spec()` to retrieve the full column specification for this data.
#> ℹ Specify the column types or set `show_col_types = FALSE` to quiet this message.
#> # A tibble: 32 × 12
#>    car        mpg   cyl  disp    hp  drat    wt  qsec    vs    am  gear
#>    <chr>    <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl>
#>  1 Mazda R…  21       6  160    110  3.9   2.62  16.5     0     1     4
#>  2 Mazda R…  21       6  160    110  3.9   2.88  17.0     0     1     4
#>  3 Datsun …  22.8     4  108     93  3.85  2.32  18.6     1     1     4
#>  4 Hornet …  21.4     6  258    110  3.08  3.22  19.4     1     0     3
#>  5 Hornet …  18.7     8  360    175  3.15  3.44  17.0     0     0     3
#>  6 Valiant   18.1     6  225    105  2.76  3.46  20.2     1     0     3
#>  7 Duster …  14.3     8  360    245  3.21  3.57  15.8     0     0     3
#>  8 Merc 24…  24.4     4  147.    62  3.69  3.19  20       1     0     4
#>  9 Merc 230  22.8     4  141.    95  3.92  3.15  22.9     1     0     4
#> 10 Merc 280  19.2     6  168.   123  3.92  3.44  18.3     1     0     4
#> # ℹ 22 more rows
#> # ℹ 1 more variable: carb <dbl>

# Column types --------------------------------------------------------------
# By default, vroom guesses the columns types, looking at 1000 rows
# throughout the dataset.
# You can specify them explicitly with a compact specification:
vroom(I("x,y\n1,2\n3,4\n"), col_types = "dc")
#> # A tibble: 2 × 2
#>       x y    
#>   <dbl> <chr>
#> 1     1 2    
#> 2     3 4    

# Or with a list of column types:
vroom(I("x,y\n1,2\n3,4\n"), col_types = list(col_double(), col_character()))
#> # A tibble: 2 × 2
#>       x y    
#>   <dbl> <chr>
#> 1     1 2    
#> 2     3 4    

# File types ----------------------------------------------------------------
# csv
vroom(I("a,b\n1.0,2.0\n"), delim = ",")
#> Rows: 1 Columns: 2
#> ── Column specification ───────────────────────────────────────────────
#> Delimiter: ","
#> dbl (2): a, b
#> 
#> ℹ Use `spec()` to retrieve the full column specification for this data.
#> ℹ Specify the column types or set `show_col_types = FALSE` to quiet this message.
#> # A tibble: 1 × 2
#>       a     b
#>   <dbl> <dbl>
#> 1     1     2
# tsv
vroom(I("a\tb\n1.0\t2.0\n"))
#> Rows: 1 Columns: 2
#> ── Column specification ───────────────────────────────────────────────
#> Delimiter: "\t"
#> dbl (2): a, b
#> 
#> ℹ Use `spec()` to retrieve the full column specification for this data.
#> ℹ Specify the column types or set `show_col_types = FALSE` to quiet this message.
#> # A tibble: 1 × 2
#>       a     b
#>   <dbl> <dbl>
#> 1     1     2
# Other delimiters
vroom(I("a|b\n1.0|2.0\n"), delim = "|")
#> Rows: 1 Columns: 2
#> ── Column specification ───────────────────────────────────────────────
#> Delimiter: "|"
#> dbl (2): a, b
#> 
#> ℹ Use `spec()` to retrieve the full column specification for this data.
#> ℹ Specify the column types or set `show_col_types = FALSE` to quiet this message.
#> # A tibble: 1 × 2
#>       a     b
#>   <dbl> <dbl>
#> 1     1     2

# Read datasets across multiple files ---------------------------------------
mtcars_by_cyl <- vroom_example(vroom_examples("mtcars-"))
mtcars_by_cyl
#> [1] "/home/runner/work/_temp/Library/vroom/extdata/mtcars-4.csv"        
#> [2] "/home/runner/work/_temp/Library/vroom/extdata/mtcars-6.csv"        
#> [3] "/home/runner/work/_temp/Library/vroom/extdata/mtcars-8.csv"        
#> [4] "/home/runner/work/_temp/Library/vroom/extdata/mtcars-multi-cyl.zip"

# Pass the filenames directly to vroom, they are efficiently combined
vroom(mtcars_by_cyl)
#> Rows: 43 Columns: 12
#> ── Column specification ───────────────────────────────────────────────
#> Delimiter: ","
#> chr  (1): model
#> dbl (11): mpg, cyl, disp, hp, drat, wt, qsec, vs, am, gear, carb
#> 
#> ℹ Use `spec()` to retrieve the full column specification for this data.
#> ℹ Specify the column types or set `show_col_types = FALSE` to quiet this message.
#> # A tibble: 43 × 12
#>    model      mpg   cyl  disp    hp  drat    wt  qsec    vs    am  gear
#>    <chr>    <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl>
#>  1 Datsun …  22.8     4 108      93  3.85  2.32  18.6     1     1     4
#>  2 Merc 24…  24.4     4 147.     62  3.69  3.19  20       1     0     4
#>  3 Merc 230  22.8     4 141.     95  3.92  3.15  22.9     1     0     4
#>  4 Fiat 128  32.4     4  78.7    66  4.08  2.2   19.5     1     1     4
#>  5 Honda C…  30.4     4  75.7    52  4.93  1.62  18.5     1     1     4
#>  6 Toyota …  33.9     4  71.1    65  4.22  1.84  19.9     1     1     4
#>  7 Toyota …  21.5     4 120.     97  3.7   2.46  20.0     1     0     3
#>  8 Fiat X1…  27.3     4  79      66  4.08  1.94  18.9     1     1     4
#>  9 Porsche…  26       4 120.     91  4.43  2.14  16.7     0     1     5
#> 10 Lotus E…  30.4     4  95.1   113  3.77  1.51  16.9     1     1     5
#> # ℹ 33 more rows
#> # ℹ 1 more variable: carb <dbl>

# If you need to extract data from the filenames, use `id` to request a
# column that reveals the underlying file path
dat <- vroom(mtcars_by_cyl, id = "source")
#> Rows: 43 Columns: 13
#> ── Column specification ───────────────────────────────────────────────
#> Delimiter: ","
#> chr  (1): model
#> dbl (11): mpg, cyl, disp, hp, drat, wt, qsec, vs, am, gear, carb
#> 
#> ℹ Use `spec()` to retrieve the full column specification for this data.
#> ℹ Specify the column types or set `show_col_types = FALSE` to quiet this message.
dat$source <- basename(dat$source)
dat
#> # A tibble: 43 × 13
#>    source   model   mpg   cyl  disp    hp  drat    wt  qsec    vs    am
#>    <chr>    <chr> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl>
#>  1 mtcars-… Dats…  22.8     4 108      93  3.85  2.32  18.6     1     1
#>  2 mtcars-… Merc…  24.4     4 147.     62  3.69  3.19  20       1     0
#>  3 mtcars-… Merc…  22.8     4 141.     95  3.92  3.15  22.9     1     0
#>  4 mtcars-… Fiat…  32.4     4  78.7    66  4.08  2.2   19.5     1     1
#>  5 mtcars-… Hond…  30.4     4  75.7    52  4.93  1.62  18.5     1     1
#>  6 mtcars-… Toyo…  33.9     4  71.1    65  4.22  1.84  19.9     1     1
#>  7 mtcars-… Toyo…  21.5     4 120.     97  3.7   2.46  20.0     1     0
#>  8 mtcars-… Fiat…  27.3     4  79      66  4.08  1.94  18.9     1     1
#>  9 mtcars-… Pors…  26       4 120.     91  4.43  2.14  16.7     0     1
#> 10 mtcars-… Lotu…  30.4     4  95.1   113  3.77  1.51  16.9     1     1
#> # ℹ 33 more rows
#> # ℹ 2 more variables: gear <dbl>, carb <dbl>
```
