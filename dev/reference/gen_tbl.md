# Generate a random tibble

This is useful for benchmarking, but also for bug reports when you
cannot share the real dataset.

## Usage

``` r
gen_tbl(
  rows,
  cols = NULL,
  col_types = NULL,
  locale = default_locale(),
  missing = 0
)
```

## Arguments

- rows:

  Number of rows to generate

- cols:

  Number of columns to generate, if `NULL` this is derived from
  `col_types`.

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

- locale:

  The locale controls defaults that vary from place to place. The
  default locale is US-centric (like R), but you can use
  [`locale()`](https://vroom.tidyverse.org/dev/reference/locale.md) to
  create your own locale that controls things like the default time
  zone, encoding, decimal mark, big mark, and day/month names.

- missing:

  The percentage (from 0 to 1) of missing data to use

## Details

There is also a family of functions to generate individual vectors of
each type.

## See also

[generators](https://vroom.tidyverse.org/dev/reference/generators.md) to
generate individual vectors.

## Examples

``` r
# random 10 x 5 table with random column types
rand_tbl <- gen_tbl(10, 5)
rand_tbl
#> # A tibble: 10 × 5
#>    X1                  X2         X3                  X4        
#>    <dttm>              <date>     <dttm>              <date>    
#>  1 2016-06-14 06:28:16 2008-02-28 2007-01-07 13:39:24 2007-05-02
#>  2 2018-06-29 22:59:16 2010-07-31 2013-09-24 09:09:05 2018-03-26
#>  3 2004-07-01 22:35:26 2020-10-17 2010-08-01 06:35:02 2003-10-20
#>  4 2001-09-08 03:11:25 2013-09-25 2009-08-24 00:15:53 2004-07-13
#>  5 2007-05-30 10:01:34 2019-12-09 2015-02-17 11:58:49 2018-02-21
#>  6 2009-01-18 00:11:12 2010-03-26 2019-12-22 08:26:43 2003-07-19
#>  7 2004-11-30 08:50:07 2006-06-04 2004-08-10 08:59:34 2016-06-19
#>  8 2009-01-26 20:18:09 2013-09-16 2005-05-04 10:53:10 2012-09-15
#>  9 2002-04-11 01:07:36 2012-05-28 2014-08-09 14:09:45 2009-11-30
#> 10 2008-10-10 11:06:51 2006-08-09 2010-12-24 01:36:44 2006-09-01
#> # ℹ 1 more variable: X5 <dttm>

# all double 25 x 4 table
dbl_tbl <- gen_tbl(25, 4, col_types = "dddd")
dbl_tbl
#> # A tibble: 25 × 4
#>         X1        X2     X3     X4
#>      <dbl>     <dbl>  <dbl>  <dbl>
#>  1 -0.872   0.000480  2.58   2.13 
#>  2  0.107   0.755    -0.789  0.704
#>  3 -0.587   0.342     0.588  0.715
#>  4 -0.328   0.168    -0.711 -1.09 
#>  5 -0.0854  1.40      1.58   0.402
#>  6 -2.05   -0.679     0.676  0.404
#>  7  0.151   0.738    -0.233  2.04 
#>  8 -0.293  -0.861     0.637  1.14 
#>  9  0.255   0.421    -1.37  -0.777
#> 10 -0.553   1.45     -1.43  -0.280
#> # ℹ 15 more rows

# Use the dots in long form column types to change the random function and options
types <- rep(times = 4, list(col_double(f = stats::runif, min = -10, max = 25)))
types
#> [[1]]
#> <collector_double>
#> 
#> [[2]]
#> <collector_double>
#> 
#> [[3]]
#> <collector_double>
#> 
#> [[4]]
#> <collector_double>
#> 
dbl_tbl2 <- gen_tbl(25, 4, col_types = types)
dbl_tbl2
#> # A tibble: 25 × 4
#>       X1    X2    X3    X4
#>    <dbl> <dbl> <dbl> <dbl>
#>  1  9.14 21.7  -4.04  6.07
#>  2 -6.64 -8.74 17.4  10.9 
#>  3  3.59 -5.40 -8.92 -4.22
#>  4 -3.97 -6.71 17.6   9.84
#>  5 14.2  14.4  -4.17 21.4 
#>  6 13.6   4.20 -9.00 10.8 
#>  7 23.1  -7.70 17.5  19.1 
#>  8 -3.13 -5.57 18.9  10.8 
#>  9 23.9  22.8  23.8  17.3 
#> 10  3.55 -2.43  3.25  3.92
#> # ℹ 15 more rows
```
