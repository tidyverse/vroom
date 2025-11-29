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
#> [1] "uX3Rn9MtoVUelMsC1KjUGn5z9" "P3eQ0mGW55S4wwHpdx7RH"    
#> [3] "3AMTCdfwY6W2T"             "2QfFHPVHD"                

# factors
gen_factor(4)
#> [1] old_springbok    rapid_lizard     clever_ram       angry_hartebeest
#> 14 Levels: powerful_chameleon clever_ram ... panicky_lion

# logical
gen_logical(4)
#> [1] FALSE  TRUE FALSE FALSE

# numbers
gen_double(4)
#> [1] -0.072655731 -0.024083976  1.584497498  0.005675627
gen_integer(4)
#> [1] 1109964342 2012064061 1272386238 1652478883

# temporal data
gen_time(4)
#> 17:25:06
#> 08:48:57
#> 12:33:44
#> 11:16:37
gen_date(4)
#> [1] "2018-09-08" "2003-11-17" "2014-05-30" "2009-11-16"
gen_datetime(4)
#> [1] "2013-12-28 15:25:30 UTC" "2004-03-07 17:59:08 UTC"
#> [3] "2006-11-16 20:23:19 UTC" "2018-10-29 20:03:46 UTC"
```
