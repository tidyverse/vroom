# Retrieve parsing problems

vroom will only fail to parse a file if the file is invalid in a way
that is unrecoverable. However there are a number of non-fatal problems
that you might want to know about. You can retrieve a data frame of
these problems with this function.

## Usage

``` r
problems(x = .Last.value, lazy = FALSE)
```

## Arguments

- x:

  A data frame from
  [`vroom::vroom()`](https://vroom.tidyverse.org/dev/reference/vroom.md).

- lazy:

  If `TRUE`, just the problems found so far are returned. If `FALSE`
  (the default) the lazy data is first read completely and all problems
  are returned.

## Value

A data frame with one row for each problem and four columns:

- row,col - Row and column number that caused the problem, referencing
  the original input

- expected - What vroom expected to find

- actual - What it actually found

- file - The file with the problem
