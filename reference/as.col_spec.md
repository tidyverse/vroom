# Coerce to a column specification

This is most useful for generating a specification using the short form
or coercing from a list.

## Usage

``` r
as.col_spec(x, call = caller_env())
```

## Arguments

- x:

  Input object

## Examples

``` r
as.col_spec("cccnnn")
#> cols(
#>   col_character(),
#>   col_character(),
#>   col_character(),
#>   col_number(),
#>   col_number(),
#>   col_number()
#> )
```
