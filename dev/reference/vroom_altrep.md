# Show which column types are using Altrep

`vroom_altrep()` can be used directly as input to the `altrep` argument
of [`vroom()`](https://vroom.tidyverse.org/dev/reference/vroom.md).

## Usage

``` r
vroom_altrep(which = NULL)
```

## Arguments

- which:

  A character vector of column types to use Altrep for. Can also take
  `TRUE` or `FALSE` to use Altrep for all possible or none of the types

## Details

Alternatively there is also a family of environment variables to control
use of the Altrep framework. These can then be set in your `.Renviron`
file, e.g. with `usethis::edit_r_environ()`. The variables can take one
of `true`, `false`, `TRUE`, `FALSE`, `1`, or `0`.

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

## Examples

``` r
vroom_altrep()
#> Using Altrep representations for:
#>  * chr
vroom_altrep(c("chr", "fct", "int"))
#> Using Altrep representations for:
#>  * chr
#>  * fct
#>  * int
vroom_altrep(TRUE)
#> Using Altrep representations for:
#>  * chr
#>  * fct
#>  * int
#>  * dbl
#>  * num
#>  * lgl
#>  * dttm
#>  * date
#>  * time
#>  * big_int
vroom_altrep(FALSE)
#> Using Altrep representations for:
#>  * 
```
