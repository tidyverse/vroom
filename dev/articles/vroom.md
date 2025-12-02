# Get started with vroom

The vroom package contains one main function
[`vroom()`](https://vroom.tidyverse.org/dev/reference/vroom.md) which is
used to read all types of delimited files. A delimited file is any file
in which the data is separated (delimited) by one or more characters.

The most common type of delimited files are CSV (Comma Separated Values)
or TSV (Tab Separated Values) files, typically these files have a `.csv`
and `.tsv` suffix respectively.

``` r
library(vroom)
```

This vignette covers the following topics:

- The basics of reading files, including
  - single files
  - multiple files
  - compressed files
  - remote files
- Skipping particular columns.
- Specifying column types, for additional safety and when the automatic
  guessing fails.
- Writing regular and compressed files

## Reading files

To read a CSV, or other type of delimited file with vroom pass the file
to [`vroom()`](https://vroom.tidyverse.org/dev/reference/vroom.md). The
delimiter will be automatically guessed if it is a common delimiter;
e.g. (“,” “” ” “\|” “:” “;”). If the guessing fails or you are using a
less common delimiter specify it with the `delim` parameter.
(e.g. `delim = ","`).

We have included an example CSV file in the vroom package for use in
examples and tests. Access it with `vroom_example("mtcars.csv")`

``` r
# See where the example file is stored on your machine
file <- vroom_example("mtcars.csv")
file
#> [1] "/home/runner/work/_temp/Library/vroom/extdata/mtcars.csv"

# Read the file, by default vroom will guess the delimiter automatically.
vroom(file)
#> Rows: 32 Columns: 12
#> ── Column specification ───────────────────────────────────────────────
#> Delimiter: ","
#> chr  (1): model
#> dbl (11): mpg, cyl, disp, hp, drat, wt, qsec, vs, am, gear, carb
#> 
#> ℹ Use `spec()` to retrieve the full column specification for this data.
#> ℹ Specify the column types or set `show_col_types = FALSE` to quiet this message.
#> # A tibble: 32 × 12
#>   model       mpg   cyl  disp    hp  drat    wt  qsec    vs    am  gear
#>   <chr>     <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl>
#> 1 Mazda RX4  21       6   160   110  3.9   2.62  16.5     0     1     4
#> 2 Mazda RX…  21       6   160   110  3.9   2.88  17.0     0     1     4
#> 3 Datsun 7…  22.8     4   108    93  3.85  2.32  18.6     1     1     4
#> # ℹ 29 more rows
#> # ℹ 1 more variable: carb <dbl>

# You can also specify it explicitly, which is (slightly) faster, and safer if
# you know how the file is delimited.
vroom(file, delim = ",")
#> Rows: 32 Columns: 12
#> ── Column specification ───────────────────────────────────────────────
#> Delimiter: ","
#> chr  (1): model
#> dbl (11): mpg, cyl, disp, hp, drat, wt, qsec, vs, am, gear, carb
#> 
#> ℹ Use `spec()` to retrieve the full column specification for this data.
#> ℹ Specify the column types or set `show_col_types = FALSE` to quiet this message.
#> # A tibble: 32 × 12
#>   model       mpg   cyl  disp    hp  drat    wt  qsec    vs    am  gear
#>   <chr>     <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl>
#> 1 Mazda RX4  21       6   160   110  3.9   2.62  16.5     0     1     4
#> 2 Mazda RX…  21       6   160   110  3.9   2.88  17.0     0     1     4
#> 3 Datsun 7…  22.8     4   108    93  3.85  2.32  18.6     1     1     4
#> # ℹ 29 more rows
#> # ℹ 1 more variable: carb <dbl>
```

## Reading multiple files

If you are reading a set of files which all have the same columns (as
in, names and types), you can pass the filenames directly to
[`vroom()`](https://vroom.tidyverse.org/dev/reference/vroom.md) and it
will combine them into one result. vroom’s example datasets include
several files named like `mtcars-i.csv`. These files contain subsets of
the `mtcars` data, for cars with different numbers of cylinders. First,
we get a character vector of these filepaths.

``` r
ve <- grep("mtcars-[0-9].csv", vroom_examples(), value = TRUE)
files <- sapply(ve, vroom_example)
files
#>                                                 mtcars-4.csv 
#> "/home/runner/work/_temp/Library/vroom/extdata/mtcars-4.csv" 
#>                                                 mtcars-6.csv 
#> "/home/runner/work/_temp/Library/vroom/extdata/mtcars-6.csv" 
#>                                                 mtcars-8.csv 
#> "/home/runner/work/_temp/Library/vroom/extdata/mtcars-8.csv"
```

Now we can efficiently read them into one table by passing the filenames
directly to vroom.

``` r
vroom(files)
#> Rows: 32 Columns: 12
#> ── Column specification ───────────────────────────────────────────────
#> Delimiter: ","
#> chr  (1): model
#> dbl (11): mpg, cyl, disp, hp, drat, wt, qsec, vs, am, gear, carb
#> 
#> ℹ Use `spec()` to retrieve the full column specification for this data.
#> ℹ Specify the column types or set `show_col_types = FALSE` to quiet this message.
#> # A tibble: 32 × 12
#>   model       mpg   cyl  disp    hp  drat    wt  qsec    vs    am  gear
#>   <chr>     <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl>
#> 1 Datsun 7…  22.8     4  108     93  3.85  2.32  18.6     1     1     4
#> 2 Merc 240D  24.4     4  147.    62  3.69  3.19  20       1     0     4
#> 3 Merc 230   22.8     4  141.    95  3.92  3.15  22.9     1     0     4
#> # ℹ 29 more rows
#> # ℹ 1 more variable: carb <dbl>
```

Often the filename or directory where the files are stored contains
information. The `id` parameter can be used to add an extra column to
the result with the full path to each file. (in this case we name the
column `path`).

``` r
vroom(files, id = "path")
#> Rows: 32 Columns: 13
#> ── Column specification ───────────────────────────────────────────────
#> Delimiter: ","
#> chr  (1): model
#> dbl (11): mpg, cyl, disp, hp, drat, wt, qsec, vs, am, gear, carb
#> 
#> ℹ Use `spec()` to retrieve the full column specification for this data.
#> ℹ Specify the column types or set `show_col_types = FALSE` to quiet this message.
#> # A tibble: 32 × 13
#>   path      model   mpg   cyl  disp    hp  drat    wt  qsec    vs    am
#>   <chr>     <chr> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl>
#> 1 /home/ru… Dats…  22.8     4  108     93  3.85  2.32  18.6     1     1
#> 2 /home/ru… Merc…  24.4     4  147.    62  3.69  3.19  20       1     0
#> 3 /home/ru… Merc…  22.8     4  141.    95  3.92  3.15  22.9     1     0
#> # ℹ 29 more rows
#> # ℹ 2 more variables: gear <dbl>, carb <dbl>
```

## Reading compressed files

vroom supports reading zip, gz, bz2 and xz compressed files
automatically, just pass the filename of the compressed file to vroom.

``` r
file <- vroom_example("mtcars.csv.gz")

vroom(file)
#> Rows: 32 Columns: 12
#> ── Column specification ───────────────────────────────────────────────
#> Delimiter: ","
#> chr  (1): model
#> dbl (11): mpg, cyl, disp, hp, drat, wt, qsec, vs, am, gear, carb
#> 
#> ℹ Use `spec()` to retrieve the full column specification for this data.
#> ℹ Specify the column types or set `show_col_types = FALSE` to quiet this message.
#> # A tibble: 32 × 12
#>   model       mpg   cyl  disp    hp  drat    wt  qsec    vs    am  gear
#>   <chr>     <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl>
#> 1 Mazda RX4  21       6   160   110  3.9   2.62  16.5     0     1     4
#> 2 Mazda RX…  21       6   160   110  3.9   2.88  17.0     0     1     4
#> 3 Datsun 7…  22.8     4   108    93  3.85  2.32  18.6     1     1     4
#> # ℹ 29 more rows
#> # ℹ 1 more variable: carb <dbl>
```

[`vroom()`](https://vroom.tidyverse.org/dev/reference/vroom.md)
decompresses, indexes and writes the decompressed data to a file in the
temp directory in a single stream. The temporary file is used to lazily
look up the values and will be automatically cleaned up when all values
in the object have been fully read, the object is removed, or the R
session ends.

### Reading individual files from a multi-file zip archive

If you are reading a zip file that contains multiple files with the same
format, you can read a subset of the files at once like so:

``` r
zip_file <- vroom_example("mtcars-multi-cyl.zip")
filenames <- unzip(zip_file, list = TRUE)$Name
filenames
#> [1] "mtcars-4.csv" "mtcars-6.csv" "mtcars-8.csv"

# imagine we only want to read 2 of the 3 files
vroom(purrr::map(filenames[c(1, 3)], \(x) unz(zip_file, x)))
#> Rows: 25 Columns: 12
#> ── Column specification ───────────────────────────────────────────────
#> Delimiter: ","
#> chr  (1): model
#> dbl (11): mpg, cyl, disp, hp, drat, wt, qsec, vs, am, gear, carb
#> 
#> ℹ Use `spec()` to retrieve the full column specification for this data.
#> ℹ Specify the column types or set `show_col_types = FALSE` to quiet this message.
#> # A tibble: 25 × 12
#>   model       mpg   cyl  disp    hp  drat    wt  qsec    vs    am  gear
#>   <chr>     <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl>
#> 1 Datsun 7…  22.8     4  108     93  3.85  2.32  18.6     1     1     4
#> 2 Merc 240D  24.4     4  147.    62  3.69  3.19  20       1     0     4
#> 3 Merc 230   22.8     4  141.    95  3.92  3.15  22.9     1     0     4
#> # ℹ 22 more rows
#> # ℹ 1 more variable: carb <dbl>
```

## Reading remote files

vroom can read files directly from the internet as well by passing the
URL of the file to vroom.

``` r
file <- "https://raw.githubusercontent.com/tidyverse/vroom/main/inst/extdata/mtcars.csv"
vroom(file)
#> Rows: 32 Columns: 12
#> ── Column specification ───────────────────────────────────────────────
#> Delimiter: ","
#> chr  (1): model
#> dbl (11): mpg, cyl, disp, hp, drat, wt, qsec, vs, am, gear, carb
#> 
#> ℹ Use `spec()` to retrieve the full column specification for this data.
#> ℹ Specify the column types or set `show_col_types = FALSE` to quiet this message.
#> # A tibble: 32 × 12
#>   model       mpg   cyl  disp    hp  drat    wt  qsec    vs    am  gear
#>   <chr>     <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl>
#> 1 Mazda RX4  21       6   160   110  3.9   2.62  16.5     0     1     4
#> 2 Mazda RX…  21       6   160   110  3.9   2.88  17.0     0     1     4
#> 3 Datsun 7…  22.8     4   108    93  3.85  2.32  18.6     1     1     4
#> # ℹ 29 more rows
#> # ℹ 1 more variable: carb <dbl>
```

It can even read gzipped files from the internet (although not the other
compressed formats).

``` r
file <- "https://raw.githubusercontent.com/tidyverse/vroom/main/inst/extdata/mtcars.csv.gz"
vroom(file)
#> Rows: 32 Columns: 12
#> ── Column specification ───────────────────────────────────────────────
#> Delimiter: ","
#> chr  (1): model
#> dbl (11): mpg, cyl, disp, hp, drat, wt, qsec, vs, am, gear, carb
#> 
#> ℹ Use `spec()` to retrieve the full column specification for this data.
#> ℹ Specify the column types or set `show_col_types = FALSE` to quiet this message.
#> # A tibble: 32 × 12
#>   model       mpg   cyl  disp    hp  drat    wt  qsec    vs    am  gear
#>   <chr>     <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl>
#> 1 Mazda RX4  21       6   160   110  3.9   2.62  16.5     0     1     4
#> 2 Mazda RX…  21       6   160   110  3.9   2.88  17.0     0     1     4
#> 3 Datsun 7…  22.8     4   108    93  3.85  2.32  18.6     1     1     4
#> # ℹ 29 more rows
#> # ℹ 1 more variable: carb <dbl>
```

## Column selection

vroom provides the same interface for column selection and renaming as
[dplyr::select()](https://dplyr.tidyverse.org/reference/select.html).
This provides very efficient, flexible and readable selections. For
example you can select by:

- A character vector of column names

``` r
file <- vroom_example("mtcars.csv.gz")

vroom(file, col_select = c(model, cyl, gear))
#> Rows: 32 Columns: 3
#> ── Column specification ───────────────────────────────────────────────
#> Delimiter: ","
#> chr (1): model
#> dbl (2): cyl, gear
#> 
#> ℹ Use `spec()` to retrieve the full column specification for this data.
#> ℹ Specify the column types or set `show_col_types = FALSE` to quiet this message.
#> # A tibble: 32 × 3
#>   model           cyl  gear
#>   <chr>         <dbl> <dbl>
#> 1 Mazda RX4         6     4
#> 2 Mazda RX4 Wag     6     4
#> 3 Datsun 710        4     4
#> # ℹ 29 more rows
```

- A numeric vector of column indexes, e.g. `c(1, 2, 5)`

``` r
vroom(file, col_select = c(1, 3, 11))
#> Rows: 32 Columns: 3
#> ── Column specification ───────────────────────────────────────────────
#> Delimiter: ","
#> chr (1): model
#> dbl (2): cyl, gear
#> 
#> ℹ Use `spec()` to retrieve the full column specification for this data.
#> ℹ Specify the column types or set `show_col_types = FALSE` to quiet this message.
#> # A tibble: 32 × 3
#>   model           cyl  gear
#>   <chr>         <dbl> <dbl>
#> 1 Mazda RX4         6     4
#> 2 Mazda RX4 Wag     6     4
#> 3 Datsun 710        4     4
#> # ℹ 29 more rows
```

- Using the selection helpers such as
  [`starts_with()`](https://tidyselect.r-lib.org/reference/starts_with.html)
  and
  [`ends_with()`](https://tidyselect.r-lib.org/reference/starts_with.html)

``` r
vroom(file, col_select = starts_with("d"))
#> Rows: 32 Columns: 2
#> ── Column specification ───────────────────────────────────────────────
#> Delimiter: ","
#> dbl (2): disp, drat
#> 
#> ℹ Use `spec()` to retrieve the full column specification for this data.
#> ℹ Specify the column types or set `show_col_types = FALSE` to quiet this message.
#> # A tibble: 32 × 2
#>    disp  drat
#>   <dbl> <dbl>
#> 1   160  3.9 
#> 2   160  3.9 
#> 3   108  3.85
#> # ℹ 29 more rows
```

- You can also rename columns

``` r
vroom(file, col_select = c(car = model, everything()))
#> Rows: 32 Columns: 12
#> ── Column specification ───────────────────────────────────────────────
#> Delimiter: ","
#> chr  (1): model
#> dbl (11): mpg, cyl, disp, hp, drat, wt, qsec, vs, am, gear, carb
#> 
#> ℹ Use `spec()` to retrieve the full column specification for this data.
#> ℹ Specify the column types or set `show_col_types = FALSE` to quiet this message.
#> # A tibble: 32 × 12
#>   car         mpg   cyl  disp    hp  drat    wt  qsec    vs    am  gear
#>   <chr>     <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl>
#> 1 Mazda RX4  21       6   160   110  3.9   2.62  16.5     0     1     4
#> 2 Mazda RX…  21       6   160   110  3.9   2.88  17.0     0     1     4
#> 3 Datsun 7…  22.8     4   108    93  3.85  2.32  18.6     1     1     4
#> # ℹ 29 more rows
#> # ℹ 1 more variable: carb <dbl>
```

## Reading fixed width files

A fixed width file can be a very compact representation of numeric data.
Unfortunately, it’s also often painful to read because you need to
describe the length of every field. vroom aims to make it as easy as
possible by providing a number of different ways to describe the field
structure. Use
[`vroom_fwf()`](https://vroom.tidyverse.org/dev/reference/vroom_fwf.md)
in conjunction with one of the following helper functions to read the
file.

``` r
fwf_sample <- vroom_example("fwf-sample.txt")
cat(readLines(fwf_sample))
#> John Smith          WA        418-Y11-4111 Mary Hartford       CA        319-Z19-4341 Evan Nolan          IL        219-532-c301
```

- [`fwf_empty()`](https://vroom.tidyverse.org/dev/reference/vroom_fwf.md) -
  Guess based on the position of empty columns.

``` r
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
```

- [`fwf_widths()`](https://vroom.tidyverse.org/dev/reference/vroom_fwf.md) -
  Use user provided set of field widths.

``` r
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
```

- [`fwf_positions()`](https://vroom.tidyverse.org/dev/reference/vroom_fwf.md) -
  Use user provided sets of start and end positions.

``` r
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
```

- [`fwf_cols()`](https://vroom.tidyverse.org/dev/reference/vroom_fwf.md) -
  Use user provided named widths.

``` r
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

- [`fwf_cols()`](https://vroom.tidyverse.org/dev/reference/vroom_fwf.md) -
  Use user provided named pairs of positions.

``` r
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
```

## Column types

vroom guesses the data types of columns as they are read, however
sometimes the guessing fails and it is necessary to explicitly set the
type of one or more columns.

The available specifications are: (with single letter abbreviations in
quotes)

- [`col_logical()`](https://vroom.tidyverse.org/dev/reference/cols.md)
  ‘l’, containing only `T`, `F`, `TRUE`, `FALSE`, `1` or `0`.
- [`col_integer()`](https://vroom.tidyverse.org/dev/reference/cols.md)
  ‘i’, integer values.
- [`col_big_integer()`](https://vroom.tidyverse.org/dev/reference/cols.md)
  ‘I’, Big integer values. (64bit integers)
- [`col_double()`](https://vroom.tidyverse.org/dev/reference/cols.md)
  ‘d’, floating point values.
- [`col_number()`](https://vroom.tidyverse.org/dev/reference/cols.md)
  ‘n’, numbers containing the `grouping_mark`
- `col_date(format = "")` ‘D’: with the locale’s `date_format`.
- `col_time(format = "")` ‘t’: with the locale’s `time_format`.
- `col_datetime(format = "")` ‘T’: ISO8601 date times.
- `col_factor(levels, ordered)` ‘f’, a fixed set of values.
- [`col_character()`](https://vroom.tidyverse.org/dev/reference/cols.md)
  ‘c’, everything else.
- [`col_skip()`](https://vroom.tidyverse.org/dev/reference/cols.md) ’\_,
  -’, don’t import this column.
- [`col_guess()`](https://vroom.tidyverse.org/dev/reference/cols.md)
  ‘?’, parse using the “best” type based on the input.

You can tell vroom what columns to use with the
[`col_types()`](https://vroom.tidyverse.org/dev/reference/cols.md)
argument in a number of ways.

If you only need to override a single column the most concise way is to
use a named vector.

``` r
# read the 'hp' columns as an integer
vroom(vroom_example("mtcars.csv"), col_types = c(hp = "i"))
#> # A tibble: 32 × 12
#>   model       mpg   cyl  disp    hp  drat    wt  qsec    vs    am  gear
#>   <chr>     <dbl> <dbl> <dbl> <int> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl>
#> 1 Mazda RX4  21       6   160   110  3.9   2.62  16.5     0     1     4
#> 2 Mazda RX…  21       6   160   110  3.9   2.88  17.0     0     1     4
#> 3 Datsun 7…  22.8     4   108    93  3.85  2.32  18.6     1     1     4
#> # ℹ 29 more rows
#> # ℹ 1 more variable: carb <dbl>

# also skip reading the 'cyl' column
vroom(vroom_example("mtcars.csv"), col_types = c(hp = "i", cyl = "_"))
#> # A tibble: 32 × 11
#>   model       mpg  disp    hp  drat    wt  qsec    vs    am  gear  carb
#>   <chr>     <dbl> <dbl> <int> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl>
#> 1 Mazda RX4  21     160   110  3.9   2.62  16.5     0     1     4     4
#> 2 Mazda RX…  21     160   110  3.9   2.88  17.0     0     1     4     4
#> 3 Datsun 7…  22.8   108    93  3.85  2.32  18.6     1     1     4     1
#> # ℹ 29 more rows

# also read the gears as a factor
vroom(vroom_example("mtcars.csv"), col_types = c(hp = "i", cyl = "_", gear = "f"))
#> # A tibble: 32 × 11
#>   model       mpg  disp    hp  drat    wt  qsec    vs    am gear   carb
#>   <chr>     <dbl> <dbl> <int> <dbl> <dbl> <dbl> <dbl> <dbl> <fct> <dbl>
#> 1 Mazda RX4  21     160   110  3.9   2.62  16.5     0     1 4         4
#> 2 Mazda RX…  21     160   110  3.9   2.88  17.0     0     1 4         4
#> 3 Datsun 7…  22.8   108    93  3.85  2.32  18.6     1     1 4         1
#> # ℹ 29 more rows
```

You can read all the columns with the same type, by using the `.default`
argument. For example reading everything as a character.

``` r
vroom(vroom_example("mtcars.csv"), col_types = c(.default = "c"))
#> # A tibble: 32 × 12
#>   model     mpg   cyl   disp  hp    drat  wt    qsec  vs    am    gear 
#>   <chr>     <chr> <chr> <chr> <chr> <chr> <chr> <chr> <chr> <chr> <chr>
#> 1 Mazda RX4 21    6     160   110   3.9   2.62  16.46 0     1     4    
#> 2 Mazda RX… 21    6     160   110   3.9   2.875 17.02 0     1     4    
#> 3 Datsun 7… 22.8  4     108   93    3.85  2.32  18.61 1     1     4    
#> # ℹ 29 more rows
#> # ℹ 1 more variable: carb <chr>
```

However you can also use the `col_*()` functions in a list.

``` r
vroom(
  vroom_example("mtcars.csv"),
  col_types = list(hp = col_integer(), cyl = col_skip(), gear = col_factor())
)
#> # A tibble: 32 × 11
#>   model       mpg  disp    hp  drat    wt  qsec    vs    am gear   carb
#>   <chr>     <dbl> <dbl> <int> <dbl> <dbl> <dbl> <dbl> <dbl> <fct> <dbl>
#> 1 Mazda RX4  21     160   110  3.9   2.62  16.5     0     1 4         4
#> 2 Mazda RX…  21     160   110  3.9   2.88  17.0     0     1 4         4
#> 3 Datsun 7…  22.8   108    93  3.85  2.32  18.6     1     1 4         1
#> # ℹ 29 more rows
```

This is most useful when a column type needs additional information,
such as for categorical data when you know all of the levels of a
factor.

``` r
vroom(
  vroom_example("mtcars.csv"),
  col_types = list(gear = col_factor(levels = c(gear = c("3", "4", "5"))))
)
#> # A tibble: 32 × 12
#>   model       mpg   cyl  disp    hp  drat    wt  qsec    vs    am gear 
#>   <chr>     <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <fct>
#> 1 Mazda RX4  21       6   160   110  3.9   2.62  16.5     0     1 4    
#> 2 Mazda RX…  21       6   160   110  3.9   2.88  17.0     0     1 4    
#> 3 Datsun 7…  22.8     4   108    93  3.85  2.32  18.6     1     1 4    
#> # ℹ 29 more rows
#> # ℹ 1 more variable: carb <dbl>
```

## Name repair

Often the names of columns in the original dataset are not ideal to work
with. [`vroom()`](https://vroom.tidyverse.org/dev/reference/vroom.md)
uses the same `.name_repair` argument as tibble, so you can use one of
the default name repair strategies or provide a custom function. A great
approach is to use the
[`janitor::make_clean_names()`](https://sfirke.github.io/janitor/reference/make_clean_names.html)
function as the input. This will automatically clean the names to use
whatever case you specify, here I am setting it to use `ALLCAPS` names.

``` r
vroom(
  vroom_example("mtcars.csv"),
  .name_repair = \(x) janitor::make_clean_names(x, case = "all_caps")
)
```

## Writing delimited files

Use
[`vroom_write()`](https://vroom.tidyverse.org/dev/reference/vroom_write.md)
to write delimited files, the default delimiter is tab, to write TSV
files. Writing to TSV by default has the following benefits: - Avoids
the issue of whether to use `;` (common in Europe) or `,` (common in the
US) - Unlikely to require quoting in fields, as very few fields contain
tabs - More easily and efficiently ingested by Unix command line tools
such as `cut`, `perl` and `awk`.

``` r
vroom_write(mtcars, "mtcars.tsv")
```

### Writing CSV delimited files

However you can also use `delim = ','` to write CSV files, which are
common as inputs to GUI spreadsheet tools like Excel or Google Sheets.

``` r
vroom_write(mtcars, "mtcars.csv", delim = ",")
```

### Writing compressed files

For gzip, bzip2 and xz compression the outputs will be automatically
compressed if the filename ends in `.gz`, `.bz2` or `.xz`.

``` r
vroom_write(mtcars, "mtcars.tsv.gz")

vroom_write(mtcars, "mtcars.tsv.bz2")

vroom_write(mtcars, "mtcars.tsv.xz")
```

It is also possible to use other compressors by using
[`pipe()`](https://rdrr.io/r/base/connections.html) with
[`vroom_write()`](https://vroom.tidyverse.org/dev/reference/vroom_write.md)
to create a pipe connection to command line utilities, such as

- [pigz](https://zlib.net/pigz/), a parallel gzip implementation
- lbzip2, a parallel bzip2 implementation
- [pixz](https://github.com/vasi/pixz), a parallel xz implementation
- [Zstandard](https://facebook.github.io/zstd/), a modern real-time
  compression algorithm.

The parallel compression versions can be considerably faster for large
output files and generally
[`vroom_write()`](https://vroom.tidyverse.org/dev/reference/vroom_write.md)
is fast enough that the compression speed becomes the bottleneck when
writing.

``` r
vroom_write(mtcars, pipe("pigz > mtcars.tsv.gz"))
```

### Reading and writing from standard input and output

vroom supports reading and writing to the C-level `stdin` and `stdout`
of the R process by using
[`stdin()`](https://rdrr.io/r/base/showConnections.html) and
[`stdout()`](https://rdrr.io/r/base/showConnections.html). E.g. from a
shell prompt you can pipe to and from vroom directly.

``` shell
cat inst/extdata/mtcars.csv | Rscript -e 'vroom::vroom(stdin())'

Rscript -e 'vroom::vroom_write(iris, stdout())' | head
```

Note this interpretation of
[`stdin()`](https://rdrr.io/r/base/showConnections.html) and
[`stdout()`](https://rdrr.io/r/base/showConnections.html) differs from
that used elsewhere by R, however we believe it better matches most
user’s expectations for this use case.

## Further reading

- [`vignette("benchmarks")`](https://vroom.tidyverse.org/dev/articles/benchmarks.md)
  discusses the performance of vroom, how it compares to alternatives
  and how it achieves its results.
- [📽 vroom: Because Life is too short to read
  slow](https://www.youtube.com/watch?v=RA9AjqZXxMU&t=10s) -
  Presentation of vroom at UseR!2019
  ([slides](https://speakerdeck.com/jimhester/vroom))
- [📹 vroom: Read and write rectangular data
  quickly](https://www.youtube.com/watch?v=ZP_y5eaAc60) - a video tour
  of the vroom features.
