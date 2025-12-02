# Create column specification

`cols()` includes all columns in the input data, guessing the column
types as the default. `cols_only()` includes only the columns you
explicitly specify, skipping the rest.

## Usage

``` r
cols(..., .default = col_guess(), .delim = NULL)

cols_only(...)

col_logical(...)

col_integer(...)

col_big_integer(...)

col_double(...)

col_character(...)

col_skip(...)

col_number(...)

col_guess(...)

col_factor(levels = NULL, ordered = FALSE, include_na = FALSE, ...)

col_datetime(format = "", ...)

col_date(format = "", ...)

col_time(format = "", ...)
```

## Arguments

- ...:

  Either column objects created by `col_*()`, or their abbreviated
  character names (as described in the `col_types` argument of
  [`vroom()`](https://vroom.tidyverse.org/reference/vroom.md)). If
  you're only overriding a few columns, it's best to refer to columns by
  name. If not named, the column types must match the column names
  exactly. In `col_*()` functions these are stored in the object.

- .default:

  Any named columns not explicitly overridden in `...` will be read with
  this column type.

- .delim:

  The delimiter to use when parsing. If the `delim` argument used in the
  call to [`vroom()`](https://vroom.tidyverse.org/reference/vroom.md) it
  takes precedence over the one specified in `col_types`.

- levels:

  Character vector of the allowed levels. When `levels = NULL` (the
  default), `levels` are discovered from the unique values of `x`, in
  the order in which they appear in `x`.

- ordered:

  Is it an ordered factor?

- include_na:

  If `TRUE` and `x` contains at least one `NA`, then `NA` is included in
  the levels of the constructed factor.

- format:

  A format specification, as described below. If set to "", date times
  are parsed as ISO8601, dates and times used the date and time formats
  specified in the
  [`locale()`](https://vroom.tidyverse.org/reference/locale.md).

  Unlike [`strptime()`](https://rdrr.io/r/base/strptime.html), the
  format specification must match the complete string.

## Details

The available specifications are: (long names in quotes and string
abbreviations in brackets)

|                               |                       |            |                                                             |
|-------------------------------|-----------------------|------------|-------------------------------------------------------------|
| function                      | long name             | short name | description                                                 |
| `col_logical()`               | "logical"             | "l"        | Logical values containing only `T`, `F`, `TRUE` or `FALSE`. |
| `col_integer()`               | "integer"             | "i"        | Integer numbers.                                            |
| `col_big_integer()`           | "big_integer"         | "I"        | Big Integers (64bit), requires the `bit64` package.         |
| `col_double()`                | "double", "numeric"   | "d"        | 64-bit double floating point numbers.                       |
| `col_character()`             | "character"           | "c"        | Character string data.                                      |
| `col_factor(levels, ordered)` | "factor"              | "f"        | A fixed set of values.                                      |
| `col_date(format = "")`       | "date"                | "D"        | Calendar dates formatted with the locale's `date_format`.   |
| `col_time(format = "")`       | "time"                | "t"        | Times formatted with the locale's `time_format`.            |
| `col_datetime(format = "")`   | "datetime", "POSIXct" | "T"        | ISO8601 date times.                                         |
| `col_number()`                | "number"              | "n"        | Human readable numbers containing the `grouping_mark`       |
| `col_skip()`                  | "skip", "NULL"        | "\_", "-"  | Skip and don't import this column.                          |
| `col_guess()`                 | "guess", "NA"         | "?"        | Parse using the "best" guessed type based on the input.     |

## Examples

``` r
cols(a = col_integer())
#> cols(
#>   a = col_integer()
#> )
cols_only(a = col_integer())
#> cols_only(
#>   a = col_integer()
#> )

# You can also use the standard abbreviations
cols(a = "i")
#> cols(
#>   a = col_integer()
#> )
cols(a = "i", b = "d", c = "_")
#> cols(
#>   a = col_integer(),
#>   b = col_double(),
#>   c = col_skip()
#> )

# Or long names (like utils::read.csv)
cols(a = "integer", b = "double", c = "skip")
#> cols(
#>   a = col_integer(),
#>   b = col_double(),
#>   c = col_skip()
#> )

# You can also use multiple sets of column definitions by combining
# them like so:

t1 <- cols(
  column_one = col_integer(),
  column_two = col_number())

t2 <- cols(
 column_three = col_character())

t3 <- t1
t3$cols <- c(t1$cols, t2$cols)
t3
#> cols(
#>   column_one = col_integer(),
#>   column_two = col_number(),
#>   column_three = col_character()
#> )
```
