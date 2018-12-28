
<!-- README.md is generated from README.Rmd. Please edit that file -->

# vroom vroom\! <a href="http://jimhester.github.io/vroom"><img src="https://i.gifer.com/2TjY.gif" align="right" /></a>

<!-- badges: start -->

[![Travis build
status](https://travis-ci.org/jimhester/vroom.svg?branch=master)](https://travis-ci.org/jimhester/vroom)
<!-- badges: end -->

The fastest delimited reader for R, **631.84 MB/sec**.

But that’s impossible\! How can it be [so
fast](https://jimhester.github.io/vroom/articles/benchmarks/benchmarks.html)?

vroom doesn’t stop to actually *read* all of your data, it simply
indexes where each record is located so it can be read later. The
vectors returned use the [Altrep
framework](https://svn.r-project.org/R/branches/ALTREP/ALTREP.html) to
lazily load the data on-demand when it is accessed, so you only pay for
what you use. It also has no (current) support for quoted fields,
comments, whitespace trimming and other niceties.

| package    | time (sec) | speedup | throughput |
| :--------- | ---------: | ------: | :--------- |
| vroom      |       2.64 |   42.12 | 631.84 MB  |
| data.table |      19.67 |    5.65 | 84.73 MB   |
| readr      |      25.82 |    4.30 | 64.55 MB   |
| read.delim |     111.12 |    1.00 | 15.00 MB   |

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
#>    <chr>  <dbl> <int> <dbl> <int> <dbl> <dbl> <dbl> <int> <int> <int> <int>
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
#> # ... with 22 more rows
```

## Thanks

  - [Gabe Becker](https://twitter.com/groundwalkergmb), [Luke
    Tierney](https://stat.uiowa.edu/~luke/) and [Tomas
    Kalibera](https://github.com/kalibera) for implementing and maintain
    the [Altrep
    framework](https://svn.r-project.org/R/branches/ALTREP/ALTREP.html)
  - [Romain François](https://twitter.com/romain_francois), whose
    [Altrepisode](https://purrple.cat/blog/2018/10/14/altrep-and-cpp/)
    package and [related
    blog-posts](https://purrple.cat/blog/2018/10/14/altrep-and-cpp/)
    were a great guide for creating new Altrep objects in C++.
  - [Matt Dowle](https://twitter.com/mattdowle) and the rest of the
    [Rdatatable](https://github.com/Rdatatable) team,
    `data.table::fread()` is blazing fast and great motivation\!
