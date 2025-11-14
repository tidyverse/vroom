# Determine whether progress bars should be shown

By default, vroom shows progress bars. However, progress reporting is
suppressed if any of the following conditions hold:

- The bar is explicitly disabled by setting the environment variable
  `VROOM_SHOW_PROGRESS` to `"false"`.

- The code is run in a non-interactive session, as determined by
  [`rlang::is_interactive()`](https://rlang.r-lib.org/reference/is_interactive.html).

- The code is run in an RStudio notebook chunk, as determined by
  `getOption("rstudio.notebook.executing")`.

## Usage

``` r
vroom_progress()
```

## Examples

``` r
vroom_progress()
#> [1] FALSE
```
