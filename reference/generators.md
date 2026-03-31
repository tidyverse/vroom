# Generate individual vectors of the types supported by vroom

Generate individual vectors of the types supported by vroom

## Usage

``` r
gen_character(n, min = 5, max = 25, values = c(letters, LETTERS, 0:9), ...)

gen_double(n, f = stats::rnorm, ...)

gen_number(n, f = stats::rnorm, ...)

gen_integer(n, min = 1L, max = .Machine$integer.max, prob = NULL, ...)

gen_factor(
  n,
  levels = NULL,
  ordered = FALSE,
  num_levels = gen_integer(1L, 1L, 25L),
  ...
)

gen_time(n, min = 0, max = hms::hms(days = 1), fractional = FALSE, ...)

gen_date(n, min = as.Date("2001-01-01"), max = as.Date("2021-01-01"), ...)

gen_datetime(
  n,
  min = as.POSIXct("2001-01-01"),
  max = as.POSIXct("2021-01-01"),
  tz = "UTC",
  ...
)

gen_logical(n, ...)

gen_name(n)
```

## Arguments

- n:

  The size of the vector to generate

- min:

  The minimum range for the vector

- max:

  The maximum range for the vector

- values:

  The explicit values to use.

- ...:

  Additional arguments passed to internal generation functions

- f:

  The random function to use.

- prob:

  a vector of probability weights for obtaining the elements of the
  vector being sampled.

- levels:

  The explicit levels to use, if `NULL` random levels are generated
  using `gen_name()`.

- ordered:

  Should the factors be ordered factors?

- num_levels:

  The number of factor levels to generate

- fractional:

  Whether to generate times with fractional seconds

- tz:

  The timezone to use for dates

## Examples

``` r
# characters
gen_character(4)
#> [1] "tt0s7u2hWXhBiIaSQ63" "acdmR"               "ctNbCy3JiOr6mzgGva" 
#> [4] "26cCjLIAiev3"       

# factors
gen_factor(4)
#> [1] bad_ape      big_puma     big_puma     panicky_newt
#> 7 Levels: old_ram ancient_gorilla obnoxious_lizard ... cooing_hartebeest

# logical
gen_logical(4)
#> [1]  TRUE  TRUE  TRUE FALSE

# numbers
gen_double(4)
#> [1] -1.0985089 -0.6331782 -2.0636545  2.6489320
gen_integer(4)
#> [1]  534125950 1574926487 1220509067  437907847

# temporal data
gen_time(4)
#> 16:56:50
#> 12:03:15
#> 12:43:06
#> 06:12:08
gen_date(4)
#> [1] "2016-07-26" "2017-10-09" "2007-06-23" "2009-04-20"
gen_datetime(4)
#> [1] "2002-06-19 19:01:30 UTC" "2018-09-12 01:07:20 UTC"
#> [3] "2017-07-15 01:06:36 UTC" "2015-07-08 16:56:14 UTC"
```
