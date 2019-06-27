
<!-- README.md is generated from README.Rmd. Please edit that file -->

# 🏎💨vroom <a href="http://r-lib.github.io/vroom"><img src="https://github.com/r-lib/vroom/raw/gh-pages/taylor.gif" align="right" width = "30%"/></a>

<!-- badges: start -->

[![Travis build
status](https://travis-ci.org/r-lib/vroom.svg?branch=master)](https://travis-ci.org/r-lib/vroom)
[![AppVeyor build
status](https://ci.appveyor.com/api/projects/status/github/r-lib/vroom?branch=master&svg=true)](https://ci.appveyor.com/project/r-lib/vroom)
[![Codecov test
coverage](https://codecov.io/gh/r-lib/vroom/branch/master/graph/badge.svg)](https://codecov.io/gh/r-lib/vroom?branch=master)
[![CRAN
status](https://www.r-pkg.org/badges/version/vroom)](https://cran.r-project.org/package=vroom)
[![Lifecycle:
maturing](https://img.shields.io/badge/lifecycle-maturing-blue.svg)](https://www.tidyverse.org/lifecycle/#maturing)
<!-- badges: end -->

The fastest delimited reader for R, **1.27 GB/sec**.

But that’s impossible\! How can it be [so
fast](http://vroom.r-lib.org/articles/benchmarks.html)?

vroom doesn’t stop to actually *read* all of your data, it simply
indexes where each record is located so it can be read later. The
vectors returned use the [Altrep
framework](https://svn.r-project.org/R/branches/ALTREP/ALTREP.html) to
lazily load the data on-demand when it is accessed, so you only pay for
what you use. This lazy access is done automatically, so no changes to
your R data-manipulation code are needed.

vroom also uses multiple threads for indexing, materializing
non-character columns, and when writing to further improve performance.

| package    | version | time (sec) | speedup | throughput |
| :--------- | ------: | ---------: | ------: | ---------: |
| vroom      |   1.0.2 |       1.33 |   56.02 |    1.27 GB |
| data.table |  1.12.2 |      15.15 |    4.91 |  110.99 MB |
| readr      |   1.3.1 |      34.81 |    2.14 |   48.31 MB |
| read.delim |   3.6.0 |      74.45 |    1.00 |   22.59 MB |

## Features

vroom has nearly all of the parsing features of
[readr](https://readr.tidyverse.org) for delimited and fixed width
files, including

  - delimiter guessing\*
  - custom delimiters (including multi-byte\* and Unicode\* delimiters)
  - specification of column types (including type guessing)
      - numeric types (double, integer, number)
      - logical types
      - datetime types (datetime, date, time)
      - categorical types (characters, factors)
  - column selection, like `dplyr::select()`\*
  - skipping headers, comments and blank lines
  - quoted fields
  - double and backslashed escapes
  - whitespace trimming
  - windows newlines
  - [reading from multiple files or
    connections\*](#reading-multiple-files)
  - embedded newlines in headers and fields\*\*
  - writing delimited files with as-needed quoting.
  - robust to invalid inputs (vroom has been extensively tested with the
    [afl](http://lcamtuf.coredump.cx/afl/) fuzz tester)\*.

\* *these are additional features only in vroom.*

\*\* *requires `num_threads = 1`.*

## Installation

Install vroom from CRAN with:

``` r
install.packages("vroom")
```

Alternatively, if you need the development version from
[GitHub](https://github.com/) install it with:

``` r
# install.packages("devtools")
devtools::install_github("r-lib/vroom")
```

## Usage

See [getting started](https://r-lib.github.io/vroom/articles/vroom.html)
to jump start your use of vroom\!

vroom uses the same interface as readr to specify column types.

``` r
vroom::vroom("mtcars.tsv",
  col_types = list(cyl = "i", gear = "f",hp = "i", disp = "_",
                   drat = "_", vs = "l", am = "l", carb = "i")
)
#> # A tibble: 32 x 10
#>   model           mpg   cyl    hp    wt  qsec vs    am    gear   carb
#>   <chr>         <dbl> <int> <int> <dbl> <dbl> <lgl> <lgl> <fct> <int>
#> 1 Mazda RX4      21       6   110  2.62  16.5 FALSE TRUE  4         4
#> 2 Mazda RX4 Wag  21       6   110  2.88  17.0 FALSE TRUE  4         4
#> 3 Datsun 710     22.8     4    93  2.32  18.6 TRUE  TRUE  4         1
#> # … with 29 more rows
```

## Reading multiple files

vroom natively supports reading from multiple files (or even multiple
connections\!).

First we generate some files to read by splitting the nycflights dataset
by airline.

``` r
library(nycflights13)
purrr::iwalk(
  split(flights, flights$carrier),
  ~ { str(.x$carrier[[1]]); vroom::vroom_write(.x, glue::glue("flights_{.y}.tsv"), delim = "\t") }
)
#>  chr "9E"
#>  chr "AA"
#>  chr "AS"
#>  chr "B6"
#>  chr "DL"
#>  chr "EV"
#>  chr "F9"
#>  chr "FL"
#>  chr "HA"
#>  chr "MQ"
#>  chr "OO"
#>  chr "UA"
#>  chr "US"
#>  chr "VX"
#>  chr "WN"
#>  chr "YV"
```

Then we can efficiently read them into one tibble by passing the
filenames directly to vroom.

``` r
files <- fs::dir_ls(glob = "flights*tsv")
files
#> flights_9E.tsv flights_AA.tsv flights_AS.tsv flights_B6.tsv flights_DL.tsv 
#> flights_EV.tsv flights_F9.tsv flights_FL.tsv flights_HA.tsv flights_MQ.tsv 
#> flights_OO.tsv flights_UA.tsv flights_US.tsv flights_VX.tsv flights_WN.tsv 
#> flights_YV.tsv
vroom::vroom(files)
#> Observations: 336,776
#> Variables: 19
#> chr  [ 4]: carrier, tailnum, origin, dest
#> dbl  [14]: year, month, day, dep_time, sched_dep_time, dep_delay, arr_time, sched_arr...
#> dttm [ 1]: time_hour
#> 
#> Call `spec()` for a copy-pastable column specification
#> Specify the column types with `col_types` to quiet this message
#> # A tibble: 336,776 x 19
#>    year month   day dep_time sched_dep_time dep_delay arr_time
#>   <dbl> <dbl> <dbl>    <dbl>          <dbl>     <dbl>    <dbl>
#> 1  2013     1     1      810            810         0     1048
#> 2  2013     1     1     1451           1500        -9     1634
#> 3  2013     1     1     1452           1455        -3     1637
#> # … with 3.368e+05 more rows, and 12 more variables: sched_arr_time <dbl>,
#> #   arr_delay <dbl>, carrier <chr>, flight <dbl>, tailnum <chr>,
#> #   origin <chr>, dest <chr>, air_time <dbl>, distance <dbl>, hour <dbl>,
#> #   minute <dbl>, time_hour <dttm>
```

## Benchmarks

The speed quoted above is from a real dataset with 14,776,615 rows and
11 columns, see the [benchmark
article](http://vroom.r-lib.org/articles/benchmarks.html) for full
details of the dataset and
[bench/](https://github.com/r-lib/vroom/blob/master/inst/bench) for the
code used to retrieve the data and perform the benchmarks.

# Environment variables

In addition to the arguments to the `vroom()` function, you can control
the behavior of vroom with a few environment variables. Generally these
will not need to be set by most users.

  - `VROOM_TEMP_PATH` - Path to the directory used to store temporary
    files when reading from a R connection. If unset defaults to the R
    session’s temporary directory (`tempdir()`).
  - `VROOM_THREADS` - The number of processor threads to use when
    indexing and parsing. If unset defaults to
    `parallel::detectCores()`.
  - `VROOM_SHOW_PROGRESS` - Whether to show the progress bar when
    indexing. Regardless of this setting the progress bar is disabled in
    non-interactive settings, R notebooks, when running tests with
    testthat and when knitting documents.
  - `VROOM_CONNECTION_SIZE` - The size (in bytes) of the connection
    buffer when reading from connections (default is 128 KiB).
  - `VROOM_WRITE_BUFFER_LINES` - The number of lines to use for each
    buffer when writing files (default: 1000).

There are also a family of variables to control use of the Altrep
framework. For versions of R where the Altrep framework is unavailable
(R \< 3.5.0) they are automatically turned off and the variables have no
effect. The variables can take one of `true`, `false`, `TRUE`, `FALSE`,
`1`, or `0`.

  - `VROOM_USE_ALTREP_NUMERICS` - If set use Altrep for *all* numeric
    types (default `false`).

There are also individual variables for each type. Currently only
`VROOM_USE_ALTREP_CHR` defaults to `true`.

  - `VROOM_USE_ALTREP_CHR`
  - `VROOM_USE_ALTREP_FCT`
  - `VROOM_USE_ALTREP_INT`
  - `VROOM_USE_ALTREP_DBL`
  - `VROOM_USE_ALTREP_NUM`
  - `VROOM_USE_ALTREP_LGL`
  - `VROOM_USE_ALTREP_DTTM`
  - `VROOM_USE_ALTREP_DATE`
  - `VROOM_USE_ALTREP_TIME`

## RStudio caveats

RStudio’s environment pane calls `object.size()` when it refreshes the
pane, which for Altrep objects can be extremely slow. RStudio 1.2.1335+
includes the fixes
([RStudio\#4210](https://github.com/rstudio/rstudio/pull/4210),
[RStudio\#4292](https://github.com/rstudio/rstudio/pull/4292)) for this
issue, so so it is recommended you use at least that version.

## Thanks

  - [Gabe Becker](https://twitter.com/groundwalkergmb), [Luke
    Tierney](https://stat.uiowa.edu/~luke/) and [Tomas
    Kalibera](https://github.com/kalibera) for conceiving, Implementing
    and maintaining the [Altrep
    framework](https://svn.r-project.org/R/branches/ALTREP/ALTREP.html)
  - [Romain François](https://twitter.com/romain_francois), whose
    [Altrepisode](https://purrple.cat/blog/2018/10/14/altrep-and-cpp/)
    package and [related
    blog-posts](https://purrple.cat/blog/2018/10/14/altrep-and-cpp/)
    were a great guide for creating new Altrep objects in C++.
  - [Matt Dowle](https://twitter.com/mattdowle) and the rest of the
    [Rdatatable](https://github.com/Rdatatable) team,
    `data.table::fread()` is blazing fast and great motivation\!
