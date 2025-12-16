# Preprocess column for output

This is a generic function that applied to each column before it is
saved to disk. It provides a hook for S3 classes that need special
handling.

## Usage

``` r
output_column(x)
```

## Arguments

- x:

  A vector

## Examples

``` r
# Most types are returned unchanged
output_column(1)
#> [1] 1
output_column("x")
#> [1] "x"

# datetimes are formatted in ISO 8601
output_column(Sys.Date())
#> [1] "2025-12-16"
output_column(Sys.time())
#> [1] "2025-12-16T20:43:29Z"
```
