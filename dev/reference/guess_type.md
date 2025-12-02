# Guess the type of a vector

Guess the type of a vector

## Usage

``` r
guess_type(
  x,
  na = c("", "NA"),
  locale = default_locale(),
  guess_integer = FALSE
)
```

## Arguments

- x:

  Character vector of values to parse.

- na:

  Character vector of strings to interpret as missing values. Set this
  option to [`character()`](https://rdrr.io/r/base/character.html) to
  indicate no missing values.

- locale:

  The locale controls defaults that vary from place to place. The
  default locale is US-centric (like R), but you can use
  [`locale()`](https://vroom.tidyverse.org/dev/reference/locale.md) to
  create your own locale that controls things like the default time
  zone, encoding, decimal mark, big mark, and day/month names.

- guess_integer:

  If `TRUE`, guess integer types for whole numbers, if `FALSE` guess
  numeric type for all numbers.

## Examples

``` r
 # Logical vectors
 guess_type(c("FALSE", "TRUE", "F", "T"))
#> <collector_logical>
 # Integers and doubles
 guess_type(c("1","2","3"))
#> <collector_double>
 guess_type(c("1.6","2.6","3.4"))
#> <collector_double>
 # Numbers containing grouping mark
 guess_type("1,234,566")
#> <collector_number>
 # ISO 8601 date times
 guess_type(c("2010-10-10"))
#> <collector_date>
 guess_type(c("2010-10-10 01:02:03"))
#> <collector_datetime>
 guess_type(c("01:02:03 AM"))
#> <collector_time>
```
