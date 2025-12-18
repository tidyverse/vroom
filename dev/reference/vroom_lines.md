# Read lines from a file

`vroom_lines()` is similar to
[`readLines()`](https://rdrr.io/r/base/readLines.html), however it reads
the lines lazily like
[`vroom()`](https://vroom.tidyverse.org/dev/reference/vroom.md), so
operations like [`length()`](https://rdrr.io/r/base/length.html),
[`head()`](https://rdrr.io/r/utils/head.html),
[`tail()`](https://rdrr.io/r/utils/head.html) and
[`sample()`](https://rdrr.io/r/base/sample.html) can be done much more
efficiently without reading all the data into R.

## Usage

``` r
vroom_lines(
  file,
  n_max = Inf,
  skip = 0,
  na = character(),
  skip_empty_rows = FALSE,
  locale = default_locale(),
  altrep = TRUE,
  num_threads = vroom_threads(),
  progress = vroom_progress()
)
```

## Arguments

- file:

  Either a path to a file, a connection, or literal data (either a
  single string or a raw vector). `file` can also be a character vector
  containing multiple filepaths or a list containing multiple
  connections.

  Files ending in `.gz`, `.bz2`, `.xz`, or `.zip` will be automatically
  uncompressed. Files starting with `http://`, `https://`, `ftp://`, or
  `ftps://` will be automatically downloaded. Remote `.gz` files can
  also be automatically downloaded and decompressed.

  Literal data is most useful for examples and tests. To be recognised
  as literal data, wrap the input with
  [`I()`](https://rdrr.io/r/base/AsIs.html).

- n_max:

  Maximum number of lines to read.

- skip:

  Number of lines to skip before reading data. If `comment` is supplied
  any commented lines are ignored *after* skipping.

- na:

  Character vector of strings to interpret as missing values. Set this
  option to [`character()`](https://rdrr.io/r/base/character.html) to
  indicate no missing values.

- skip_empty_rows:

  Should blank rows be ignored altogether? i.e. If this option is `TRUE`
  then blank rows will not be represented at all. If it is `FALSE` then
  they will be represented by `NA` values in all the columns.

- locale:

  The locale controls defaults that vary from place to place. The
  default locale is US-centric (like R), but you can use
  [`locale()`](https://vroom.tidyverse.org/dev/reference/locale.md) to
  create your own locale that controls things like the default time
  zone, encoding, decimal mark, big mark, and day/month names.

- altrep:

  Control which column types use Altrep representations, either a
  character vector of types, `TRUE` or `FALSE`. See
  [`vroom_altrep()`](https://vroom.tidyverse.org/dev/reference/vroom_altrep.md)
  for for full details.

- num_threads:

  Number of threads to use when reading and materializing vectors. If
  your data contains newlines within fields the parser will
  automatically be forced to use a single thread only.

- progress:

  Display a progress bar? By default it will only display in an
  interactive session and not while executing in an RStudio notebook
  chunk. The display of the progress bar can be disabled by setting the
  environment variable `VROOM_SHOW_PROGRESS` to `"false"`.

## Examples

``` r
lines <- vroom_lines(vroom_example("mtcars.csv"))

length(lines)
#> [1] 33
head(lines, n = 2)
#> [1] "model,mpg,cyl,disp,hp,drat,wt,qsec,vs,am,gear,carb"
#> [2] "Mazda RX4,21,6,160,110,3.9,2.62,16.46,0,1,4,4"     
tail(lines, n = 2)
#> [1] "Maserati Bora,15,8,301,335,3.54,3.57,14.6,0,1,5,8"
#> [2] "Volvo 142E,21.4,4,121,109,4.11,2.78,18.6,1,1,4,2" 
sample(lines, size = 2)
#> [1] "Porsche 914-2,26,4,120.3,91,4.43,2.14,16.7,0,1,5,2"
#> [2] "model,mpg,cyl,disp,hp,drat,wt,qsec,vs,am,gear,carb"
```
