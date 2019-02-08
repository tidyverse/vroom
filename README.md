
<!-- README.md is generated from README.Rmd. Please edit that file -->

# vroom vroom\! <a href="http://jimhester.github.io/vroom"><img src="https://i.gifer.com/2TjY.gif" align="right" /></a>

<!-- badges: start -->

[![Travis build
status](https://travis-ci.org/jimhester/vroom.svg?branch=master)](https://travis-ci.org/jimhester/vroom)
[![AppVeyor build
status](https://ci.appveyor.com/api/projects/status/github/jimhester/vroom?branch=master&svg=true)](https://ci.appveyor.com/project/jimhester/vroom)
[![Lifecycle:
experimental](https://img.shields.io/badge/lifecycle-experimental-orange.svg)](https://www.tidyverse.org/lifecycle/#experimental)
[![Codecov test
coverage](https://codecov.io/gh/jimhester/vroom/branch/master/graph/badge.svg)](https://codecov.io/gh/jimhester/vroom?branch=master)
<!-- badges: end -->

The fastest delimited reader for R, **971.70 MB/sec**.

But that’s impossible\! How can it be [so
fast](https://jimhester.github.io/vroom/articles/benchmarks/benchmarks.html)?

vroom doesn’t stop to actually *read* all of your data, it simply
indexes where each record is located so it can be read later. The
vectors returned use the [Altrep
framework](https://svn.r-project.org/R/branches/ALTREP/ALTREP.html) to
lazily load the data on-demand when it is accessed, so you only pay for
what you use.

vroom uses multiple threads for indexing and materializing non-character
vectors, to further improve performance.

However it has no (current) support for windows newlines, embedded
newlines or other niceties which can slow down and complicate parsing.

| package    | time (sec) | speedup | throughput |
| :--------- | ---------: | ------: | :--------- |
| vroom      |       1.72 |   65.88 | 971.70 MB  |
| data.table |      19.37 |    5.83 | 86.03 MB   |
| readr      |      25.71 |    4.40 | 64.84 MB   |
| read.delim |     113.02 |    1.00 | 14.75 MB   |

## Installation

Install the development version from [GitHub](https://github.com/) with:

``` r
# install.packages("devtools")
devtools::install_github("jimhester/vroom")
```

## Example

``` r
vroom::vroom("mtcars.tsv")
#> # A tibble: 32 x 12
#>    model    mpg   cyl  disp    hp  drat    wt  qsec    vs    am  gear  carb
#>    <chr>  <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl>
#>  1 Mazda…  21       6  160    110  3.9   2.62  16.5     0     1     4     4
#>  2 Mazda…  21       6  160    110  3.9   2.88  17.0     0     1     4     4
#>  3 Datsu…  22.8     4  108     93  3.85  2.32  18.6     1     1     4     1
#>  4 Horne…  21.4     6  258    110  3.08  3.22  19.4     1     0     3     1
#>  5 Horne…  18.7     8  360    175  3.15  3.44  17.0     0     0     3     2
#>  6 Valia…  18.1     6  225    105  2.76  3.46  20.2     1     0     3     1
#>  7 Duste…  14.3     8  360    245  3.21  3.57  15.8     0     0     3     4
#>  8 Merc …  24.4     4  147.    62  3.69  3.19  20       1     0     4     2
#>  9 Merc …  22.8     4  141.    95  3.92  3.15  22.9     1     0     4     2
#> 10 Merc …  19.2     6  168.   123  3.92  3.44  18.3     1     0     4     4
#> # … with 22 more rows
```

## Reading multiple files

vroom natively supports reading from multiple files (or even multiple
connections).

First we will create some files to read by splitting the nycflights
dataset by airline.

``` r
library(nycflights13)
iwalk(
  split(flights, flights$carrier),
  ~ readr::write_tsv(.x, glue::glue("flights_{.y}.tsv"))
)
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
vroom(files)
#> # A tibble: 336,776 x 19
#>     year month   day dep_time sched_dep_time dep_delay arr_time
#>    <dbl> <dbl> <dbl>    <dbl>          <dbl>     <dbl>    <dbl>
#>  1  2013     1     1      810            810         0     1048
#>  2  2013     1     1     1451           1500        -9     1634
#>  3  2013     1     1     1452           1455        -3     1637
#>  4  2013     1     1     1454           1500        -6     1635
#>  5  2013     1     1     1507           1515        -8     1651
#>  6  2013     1     1     1530           1530         0     1650
#>  7  2013     1     1     1546           1540         6     1753
#>  8  2013     1     1     1550           1550         0     1844
#>  9  2013     1     1     1552           1600        -8     1749
#> 10  2013     1     1     1554           1600        -6     1701
#> # … with 336,766 more rows, and 12 more variables: sched_arr_time <dbl>,
#> #   arr_delay <dbl>, carrier <chr>, flight <dbl>, tailnum <chr>,
#> #   origin <chr>, dest <chr>, air_time <dbl>, distance <dbl>, hour <dbl>,
#> #   minute <dbl>, time_hour <chr>
```

## Benchmarks

The speed quoted above is from a dataset with 14,776,615 rows and 11
columns, see the [benchmark
article](https://jimhester.github.io/vroom/articles/benchmarks/benchmarks.html)
for full details.

## RStudio Caveats

Until very recently (2019-01-23) RStudio’s environment pane caused
Altrep objects to be automatically materialized, which removes most of
the benefits (and can acutally make things much slower). This was fixed
in [rstudio\#4210](https://github.com/rstudio/rstudio/pull/4210), so it
is recommended you use a [daily version](https://dailies.rstudio.com/)
if you are trying to use vroom inside RStudio.

## Thanks

  - [Gabe Becker](https://twitter.com/groundwalkergmb), [Luke
    Tierney](https://stat.uiowa.edu/~luke/) and [Tomas
    Kalibera](https://github.com/kalibera) for implementing and
    maintaining the [Altrep
    framework](https://svn.r-project.org/R/branches/ALTREP/ALTREP.html)
  - [Romain François](https://twitter.com/romain_francois), whose
    [Altrepisode](https://purrple.cat/blog/2018/10/14/altrep-and-cpp/)
    package and [related
    blog-posts](https://purrple.cat/blog/2018/10/14/altrep-and-cpp/)
    were a great guide for creating new Altrep objects in C++.
  - [Matt Dowle](https://twitter.com/mattdowle) and the rest of the
    [Rdatatable](https://github.com/Rdatatable) team,
    `data.table::fread()` is blazing fast and great motivation\!
