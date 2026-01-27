# Examine the column specifications for a data frame

`cols_condense()` takes a spec object and condenses its definition by
setting the default column type to the most frequent type and only
listing columns with a different type.

`spec()` extracts the full column specification from a tibble created by
vroom.

## Usage

``` r
cols_condense(x)

spec(x)
```

## Arguments

- x:

  The data frame object to extract from

## Value

A col_spec object.

## Examples

``` r
df <- vroom(vroom_example("mtcars.csv"))
#> Rows: 32 Columns: 12
#> ── Column specification ───────────────────────────────────────────────
#> Delimiter: ","
#> chr  (1): model
#> dbl (11): mpg, cyl, disp, hp, drat, wt, qsec, vs, am, gear, carb
#> 
#> ℹ Use `spec()` to retrieve the full column specification for this data.
#> ℹ Specify the column types or set `show_col_types = FALSE` to quiet this message.
s <- spec(df)
s
#> cols(
#>   model = col_character(),
#>   mpg = col_double(),
#>   cyl = col_double(),
#>   disp = col_double(),
#>   hp = col_double(),
#>   drat = col_double(),
#>   wt = col_double(),
#>   qsec = col_double(),
#>   vs = col_double(),
#>   am = col_double(),
#>   gear = col_double(),
#>   carb = col_double(),
#>   .delim = ","
#> )

cols_condense(s)
#> cols(
#>   .default = col_double(),
#>   model = col_character(),
#>   .delim = ","
#> )
```
