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

  - I = big integer

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
#>         X1      X2 X3         X4                  X5      
#>      <dbl>   <dbl> <date>     <dttm>              <time>  
#>  1  0.971   0.629  2007-05-02 2016-08-02 20:23:17 22:44:46
#>  2 -1.01    2.07   2018-03-26 2005-01-31 12:32:49 13:01:10
#>  3 -0.0843 -1.63   2003-10-20 2015-04-09 08:48:39 13:04:13
#>  4 -0.554   0.512  2004-07-13 2002-04-22 09:41:19 06:41:10
#>  5  0.747  -1.86   2018-02-21 2008-02-01 11:32:09 10:43:15
#>  6 -0.935  -0.522  2003-07-19 2017-07-04 01:57:44 08:54:58
#>  7 -0.467  -0.0526 2016-06-19 2006-06-24 05:48:53 00:40:24
#>  8 -0.857   0.543  2012-09-15 2012-05-27 04:16:50 11:11:01
#>  9 -1.52   -0.914  2009-11-30 2007-09-19 10:16:09 09:21:38
#> 10  1.97    0.468  2006-09-01 2012-12-04 16:47:31 00:28:53

# all double 25 x 4 table
dbl_tbl <- gen_tbl(25, 4, col_types = "dddd")
dbl_tbl
#> # A tibble: 25 × 4
#>         X1     X2      X3     X4
#>      <dbl>  <dbl>   <dbl>  <dbl>
#>  1 -0.313  -0.246  0.434  -0.442
#>  2  1.07   -1.18  -0.382   0.569
#>  3  0.0700 -0.976  0.424   2.13 
#>  4 -0.639   1.07   1.06    0.425
#>  5 -0.0500  0.132  1.05   -1.68 
#>  6 -0.251   0.489 -0.0381  0.249
#>  7  0.445  -1.70   0.486   1.07 
#>  8  2.76   -1.47   1.67    2.04 
#>  9  0.0465  0.284 -0.354   0.449
#> 10  0.578   1.34   0.946   1.39 
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
#>       X1      X2    X3    X4
#>    <dbl>   <dbl> <dbl> <dbl>
#>  1 18.5  -2.86   11.1  16.0 
#>  2 -7.52  9.20   18.2   1.12
#>  3  8.44 19.0    -8.72 -6.09
#>  4 16.7  -5.54   15.7  -6.46
#>  5  5.24 -0.858  -2.47 18.0 
#>  6  9.34  0.0946 -9.44  3.30
#>  7 -2.86 -9.48   -5.50 -8.16
#>  8 -8.91 19.9    14.0  24.5 
#>  9 23.9   4.49   12.5  11.1 
#> 10 -3.75 16.3     1.46 -4.79
#> # ℹ 15 more rows
```
