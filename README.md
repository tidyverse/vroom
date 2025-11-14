
<!-- README.md is generated from README.Rmd. Please edit that file -->

# 🏎💨vroom <a href='https://vroom.r-lib.org'><img src='man/figures/logo.png' align="right" height="135" /></a>

<!-- badges: start -->

[![R-CMD-check](https://github.com/tidyverse/vroom/actions/workflows/R-CMD-check.yaml/badge.svg)](https://github.com/tidyverse/vroom/actions/workflows/R-CMD-check.yaml)
[![Codecov test
coverage](https://codecov.io/gh/tidyverse/vroom/branch/main/graph/badge.svg)](https://app.codecov.io/gh/tidyverse/vroom?branch=main)
[![CRAN
status](https://www.r-pkg.org/badges/version/vroom)](https://cran.r-project.org/package=vroom)
[![Lifecycle:
stable](https://img.shields.io/badge/lifecycle-stable-brightgreen.svg)](https://lifecycle.r-lib.org/articles/stages.html#stable)
[![Codecov test
coverage](https://codecov.io/gh/tidyverse/vroom/graph/badge.svg)](https://app.codecov.io/gh/tidyverse/vroom)
<!-- badges: end -->

The fastest delimited reader for R, **1.23 GB/sec**.

<img src="https://raw.githubusercontent.com/tidyverse/vroom/main/img/taylor.gif" align="right" />

But that’s impossible! How can it be [so
fast](https://vroom.r-lib.org/articles/benchmarks.html)?

vroom doesn’t stop to actually *read* all of your data, it simply
indexes where each record is located so it can be read later. The
vectors returned use the [Altrep
framework](https://svn.r-project.org/R/branches/ALTREP/ALTREP.html) to
lazily load the data on-demand when it is accessed, so you only pay for
what you use. This lazy access is done automatically, so no changes to
your R data-manipulation code are needed.

vroom also uses multiple threads for indexing, materializing
non-character columns, and when writing to further improve performance.

| package    | version | time (sec) | speedup |    throughput |
|:-----------|--------:|-----------:|--------:|--------------:|
| vroom      |   1.5.1 |       1.36 |   53.30 |   1.23 GB/sec |
| data.table |  1.14.0 |       5.83 |   12.40 | 281.65 MB/sec |
| readr      |   1.4.0 |      37.30 |    1.94 |  44.02 MB/sec |
| read.delim |   4.1.0 |      72.31 |    1.00 |  22.71 MB/sec |

## Features

vroom has nearly all of the parsing features of
[readr](https://readr.tidyverse.org) for delimited and fixed width
files, including

- delimiter guessing\*
- custom delimiters (including multi-byte\* and Unicode\* delimiters)
- specification of column types (including type guessing)
  - numeric types (double, integer, big integer\*, number)
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
  [afl](https://lcamtuf.coredump.cx/afl/) fuzz tester)\*.

\* *these are additional features not in readr.*

\*\* *requires `num_threads = 1`.*

## Installation

Install vroom from CRAN with:

``` r
install.packages("vroom")
```

Alternatively, if you need the development version from
[GitHub](https://github.com/) install it with:

``` r
# install.packages("pak")
pak::pak("tidyverse/vroom")
```

## Usage

See [getting started](https://vroom.r-lib.org/articles/vroom.html) to
jump start your use of vroom!

vroom uses the same interface as readr to specify column types.

``` r
vroom::vroom("mtcars.tsv",
  col_types = list(cyl = "i", gear = "f",hp = "i", disp = "_",
                   drat = "_", vs = "l", am = "l", carb = "i")
)
#> # A tibble: 32 × 10
#>   model           mpg   cyl    hp    wt  qsec vs    am    gear   carb
#>   <chr>         <dbl> <int> <int> <dbl> <dbl> <lgl> <lgl> <fct> <int>
#> 1 Mazda RX4      21       6   110  2.62  16.5 FALSE TRUE  4         4
#> 2 Mazda RX4 Wag  21       6   110  2.88  17.0 FALSE TRUE  4         4
#> 3 Datsun 710     22.8     4    93  2.32  18.6 TRUE  TRUE  4         1
#> # ℹ 29 more rows
```

## Reading multiple files

vroom natively supports reading from multiple files (or even multiple
connections!).

First we generate some files to read by splitting the nycflights dataset
by airline. For the sake of the example, we’ll just take the first 2
lines of each file.

``` r
library(nycflights13)
purrr::iwalk(
  split(flights, flights$carrier),
  ~ { .x$carrier[[1]]; vroom::vroom_write(head(.x, 2), glue::glue("flights_{.y}.tsv"), delim = "\t") }
)
```

Then we can efficiently read them into one tibble by passing the
filenames directly to vroom. The `id` argument can be used to request a
column that reveals the filename that each row originated from.

``` r
files <- fs::dir_ls(glob = "flights*tsv")
files
#> flights_9E.tsv flights_AA.tsv flights_AS.tsv flights_B6.tsv flights_DL.tsv 
#> flights_EV.tsv flights_F9.tsv flights_FL.tsv flights_HA.tsv flights_MQ.tsv 
#> flights_OO.tsv flights_UA.tsv flights_US.tsv flights_VX.tsv flights_WN.tsv 
#> flights_YV.tsv
vroom::vroom(files, id = "source")
#> Rows: 32 Columns: 20
#> ── Column specification ────────────────────────────────────────────────────────
#> Delimiter: "\t"
#> chr   (4): carrier, tailnum, origin, dest
#> dbl  (14): year, month, day, dep_time, sched_dep_time, dep_delay, arr_time, ...
#> dttm  (1): time_hour
#> 
#> ℹ Use `spec()` to retrieve the full column specification for this data.
#> ℹ Specify the column types or set `show_col_types = FALSE` to quiet this message.
#> # A tibble: 32 × 20
#>   source          year month   day dep_time sched_dep_time dep_delay arr_time
#>   <chr>          <dbl> <dbl> <dbl>    <dbl>          <dbl>     <dbl>    <dbl>
#> 1 flights_9E.tsv  2013     1     1      810            810         0     1048
#> 2 flights_9E.tsv  2013     1     1     1451           1500        -9     1634
#> 3 flights_AA.tsv  2013     1     1      542            540         2      923
#> # ℹ 29 more rows
#> # ℹ 12 more variables: sched_arr_time <dbl>, arr_delay <dbl>, carrier <chr>,
#> #   flight <dbl>, tailnum <chr>, origin <chr>, dest <chr>, air_time <dbl>,
#> #   distance <dbl>, hour <dbl>, minute <dbl>, time_hour <dttm>
```

## Learning more

- [Getting started with
  vroom](https://vroom.r-lib.org/articles/vroom.html)
- [📽 vroom: Because Life is too short to read
  slow](https://www.youtube.com/watch?v=RA9AjqZXxMU&t=10s) -
  Presentation at UseR!2019
  ([slides](https://speakerdeck.com/jimhester/vroom))
- [📹 vroom: Read and write rectangular data
  quickly](https://www.youtube.com/watch?v=ZP_y5eaAc60) - a video tour
  of the vroom features.

## Benchmarks

The speed quoted above is from a real 1.53G dataset with 14,388,451 rows
and 11 columns, see the [benchmark
article](https://vroom.r-lib.org/articles/benchmarks.html) for full
details of the dataset and
[bench/](https://github.com/tidyverse/vroom/tree/main/inst/bench) for
the code used to retrieve the data and perform the benchmarks.

# Environment variables

In addition to the arguments to the `vroom()` function, you can control
the behavior of vroom with a few environment variables. Generally these
will not need to be set by most users.

- `VROOM_TEMP_PATH` - Path to the directory used to store temporary
  files when reading from a R connection. If unset defaults to the R
  session’s temporary directory (`tempdir()`).
- `VROOM_THREADS` - The number of processor threads to use when indexing
  and parsing. If unset defaults to `parallel::detectCores()`.
- `VROOM_SHOW_PROGRESS` - Whether to show the progress bar when
  indexing. Regardless of this setting the progress bar is disabled in
  non-interactive settings, R notebooks, when running tests with
  testthat and when knitting documents.
- `VROOM_CONNECTION_SIZE` - The size (in bytes) of the connection buffer
  when reading from connections (default is 128 KiB).
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
- `VROOM_USE_ALTREP_BIG_INT`
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
([RStudio#4210](https://github.com/rstudio/rstudio/pull/4210),
[RStudio#4292](https://github.com/rstudio/rstudio/pull/4292)) for this
issue, so it is recommended you use at least that version.

## Thanks

- [Gabe Becker](https://github.com/gmbecker), [Luke
  Tierney](https://homepage.divms.uiowa.edu/~luke/) and [Tomas
  Kalibera](https://github.com/kalibera) for conceiving, Implementing
  and maintaining the [Altrep
  framework](https://svn.r-project.org/R/branches/ALTREP/ALTREP.html)
- [Romain François](https://github.com/romainfrancois), whose
  [Altrepisode](https://web.archive.org/web/20200315075838/https://purrple.cat/blog/2018/10/14/altrep-and-cpp/)
  package and [related
  blog-posts](https://web.archive.org/web/20200315075838/https://purrple.cat/blog/2018/10/14/altrep-and-cpp/)
  were a great guide for creating new Altrep objects in C++.
- [Matt Dowle](https://github.com/mattdowle) and the rest of the
  [Rdatatable](https://github.com/Rdatatable) team,
  `data.table::fread()` is blazing fast and great motivation to see how
  fast we could go faster!
