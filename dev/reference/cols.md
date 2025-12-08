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
  [`vroom()`](https://vroom.tidyverse.org/dev/reference/vroom.md)). If
  you're only overriding a few columns, it's best to refer to columns by
  name. If not named, the column types must match the column names
  exactly. In `col_*()` functions these are stored in the object.

- .default:

  Any named columns not explicitly overridden in `...` will be read with
  this column type.

- .delim:

  The delimiter to use when parsing. If the `delim` argument used in the
  call to
  [`vroom()`](https://vroom.tidyverse.org/dev/reference/vroom.md) it
  takes precedence over the one specified in `col_types`.

- levels:

  Character vector of the allowed levels. When `levels = NULL` (the
  default), `levels` are discovered from the unique values of the data,
  in the order in which they are encountered.

- ordered:

  Is it an ordered factor?

- include_na:

  If `TRUE` and the data contains at least one `NA`, then `NA` is
  included in the levels of the constructed factor.

- format:

  A format specification. If set to "":

  - `col_datetime()` expects ISO8601 datetimes. Here are some examples
    of input that should just work: "2024-01-15", "2024-01-15 14:30:00",
    "2024-01-15T14:30:00Z".

  - `col_date()` uses the `date_format` from
    [`locale()`](https://vroom.tidyverse.org/dev/reference/locale.md)
    (default `"%AD"`). These inputs should just work: "2024-01-15",
    "01/15/2024".

  - `col_time()` uses the `time_format` from
    [`locale()`](https://vroom.tidyverse.org/dev/reference/locale.md)
    (default `"%AT"`). These inputs should just work: "14:30:00",
    "2:30:00 PM".

  Unlike [`strptime()`](https://rdrr.io/r/base/strptime.html), the
  format specification must match the complete string. For more details,
  see below.

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

### Date, time, and datetime formats:

vroom uses a format specification similar to
[`strptime()`](https://rdrr.io/r/base/strptime.html). There are three
types of element:

1.  A conversion specification that is "%" followed by a letter. For
    example "%Y" matches a 4 digit year, "%m", matches a 2 digit month
    and "%d" matches a 2 digit day. Month and day default to `1`, (i.e.
    Jan 1st) if not present, for example if only a year is given.

2.  Whitespace is any sequence of zero or more whitespace characters.

3.  Any other character is matched exactly.

vroom's datetime `col_*()` functions recognize the following
specifications:

- Year: "%Y" (4 digits). "%y" (2 digits); 00-69 -\> 2000-2069, 70-99 -\>
  1970-1999.

- Month: "%m" (2 digits), "%b" (abbreviated name in current locale),
  "%B" (full name in current locale).

- Day: "%d" (2 digits), "%e" (optional leading space), "%a" (abbreviated
  name in current locale).

- Hour: "%H" or "%I" or "%h", use I (and not H) with AM/PM, use h (and
  not H) if your times represent durations longer than one day.

- Minutes: "%M"

- Seconds: "%S" (integer seconds), "%OS" (partial seconds)

- Time zone: "%Z" (as name, e.g. "America/Chicago"), "%z" (as offset
  from UTC, e.g. "+0800")

- AM/PM indicator: "%p".

- Non-digits: "%." skips one non-digit character, "%+" skips one or more
  non-digit characters, "%\*" skips any number of non-digits characters.

- Automatic parsers: "%AD" parses with a flexible YMD parser, "%AT"
  parses with a flexible HMS parser.

- Shortcuts: "%D" = "%m/%d/%y", "%F" = "%Y-%m-%d", "%R" = "%H:%M", "%T"
  = "%H:%M:%S", "%x" = "%y/%m/%d".

#### ISO8601 support

Currently, vroom does not support all of ISO8601. Missing features:

- Week & weekday specifications, e.g. "2013-W05", "2013-W05-10".

- Ordinal dates, e.g. "2013-095".

- Using commas instead of a period for decimal separator.

The parser is also a little laxer than ISO8601:

- Dates and times can be separated with a space, not just T.

- Mostly correct specifications like "2009-05-19 14:" and "200912-01"
  work.

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
