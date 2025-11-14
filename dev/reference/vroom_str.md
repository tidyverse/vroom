# Structure of objects

Similar to [`str()`](https://rdrr.io/r/utils/str.html) but with more
information for Altrep objects.

## Usage

``` r
vroom_str(x)
```

## Arguments

- x:

  a vector

## Examples

``` r
# when used on non-altrep objects altrep will always be false
vroom_str(mtcars)
#> 'data.frame': 32 obs., 11 vars.:
#> $mpg:    altrep:false    type: double    length:32
#> $cyl:    altrep:false    type: double    length:32
#> $disp:   altrep:false    type: double    length:32
#> $hp: altrep:false    type: double    length:32
#> $drat:   altrep:false    type: double    length:32
#> $wt: altrep:false    type: double    length:32
#> $qsec:   altrep:false    type: double    length:32
#> $vs: altrep:false    type: double    length:32
#> $am: altrep:false    type: double    length:32
#> $gear:   altrep:false    type: double    length:32
#> $carb:   altrep:false    type: double    length:32

mt <- vroom(vroom_example("mtcars.csv"), ",", altrep = c("chr", "dbl"))
#> Rows: 32 Columns: 12
#> ── Column specification ───────────────────────────────────────────────
#> Delimiter: ","
#> chr  (1): model
#> dbl (11): mpg, cyl, disp, hp, drat, wt, qsec, vs, am, gear, carb
#> 
#> ℹ Use `spec()` to retrieve the full column specification for this data.
#> ℹ Specify the column types or set `show_col_types = FALSE` to quiet this message.
vroom_str(mt)
#> 'spec_tbl_df', 'tbl_df', 'tbl', and 'data.frame': 32 obs., 12 vars.:
#> $model:  altrep:true type:vroom::vroom_chr   length:32   materialized:false
#> $mpg:    altrep:true type:vroom::vroom_dbl   length:32   materialized:false
#> $cyl:    altrep:true type:vroom::vroom_dbl   length:32   materialized:false
#> $disp:   altrep:true type:vroom::vroom_dbl   length:32   materialized:false
#> $hp: altrep:true type:vroom::vroom_dbl   length:32   materialized:false
#> $drat:   altrep:true type:vroom::vroom_dbl   length:32   materialized:false
#> $wt: altrep:true type:vroom::vroom_dbl   length:32   materialized:false
#> $qsec:   altrep:true type:vroom::vroom_dbl   length:32   materialized:false
#> $vs: altrep:true type:vroom::vroom_dbl   length:32   materialized:false
#> $am: altrep:true type:vroom::vroom_dbl   length:32   materialized:false
#> $gear:   altrep:true type:vroom::vroom_dbl   length:32   materialized:false
#> $carb:   altrep:true type:vroom::vroom_dbl   length:32   materialized:false
```
