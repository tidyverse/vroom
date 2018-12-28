
<!-- README.md is generated from README.Rmd. Please edit that file -->

# vroom voom\! <a href="http://jimhester.github.io/vroom"><img src="https://i.gifer.com/2TjY.gif" align="right" /></a>

<!-- badges: start -->

[![Travis build
status](https://travis-ci.org/jimhester/vroom.svg?branch=master)](https://travis-ci.org/jimhester/vroom)
<!-- badges: end -->

The fastest delimited reader for R, **605.51 MB/sec**.

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
| vroom      |       2.75 |   41.05 | 605.51 MB  |
| data.table |      20.45 |    5.53 | 81.50 MB   |
| readr      |      25.95 |    4.35 | 64.23 MB   |
| read.delim |     113.00 |    1.00 | 14.75 MB   |

## Installation

Install the development version from [GitHub](https://github.com/) with:

``` r
# install.packages("devtools")
devtools::install_github("jimhester/vroom")
```

## Example

This is a basic example which shows you how to solve a common problem:

``` r
vroom::vroom("mtcars.tsv")
#> # A tibble: 32 x 12
#>      mpg   cyl  disp    hp  drat    wt  qsec    vs    am  gear  carb test 
#>    <dbl> <int> <dbl> <int> <dbl> <dbl> <dbl> <int> <int> <int> <int> <lgl>
#>  1  21       6  160    110  3.9   2.62  16.5     0     1     4     4 FALSE
#>  2  21       6  160    110  3.9   2.88  17.0     0     1     4     4 FALSE
#>  3  22.8     4  108     93  3.85  2.32  18.6     1     1     4     1 TRUE 
#>  4  21.4     6  258    110  3.08  3.22  19.4     1     0     3     1 FALSE
#>  5  18.7     8  360    175  3.15  3.44  17.0     0     0     3     2 FALSE
#>  6  18.1     6  225    105  2.76  3.46  20.2     1     0     3     1 FALSE
#>  7  14.3     8  360    245  3.21  3.57  15.8     0     0     3     4 FALSE
#>  8  24.4     4  147.    62  3.69  3.19  20       1     0     4     2 TRUE 
#>  9  22.8     4  141.    95  3.92  3.15  22.9     1     0     4     2 TRUE 
#> 10  19.2     6  168.   123  3.92  3.44  18.3     1     0     4     4 FALSE
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
