# Get path to vroom examples

vroom comes bundled with a number of sample files in its 'inst/extdata'
directory. Use `vroom_examples()` to list all the available examples and
`vroom_example()` to retrieve the path to one example.

## Usage

``` r
vroom_example(path)

vroom_examples(pattern = NULL)
```

## Arguments

- path:

  Name of file.

- pattern:

  A regular expression of filenames to match. If `NULL`, all available
  files are returned.

## Examples

``` r
# List all available examples
vroom_examples()
#>  [1] "fwf-sample.txt"       "mtcars-4.csv"        
#>  [3] "mtcars-6.csv"         "mtcars-8.csv"        
#>  [5] "mtcars-multi-cyl.zip" "mtcars.csv"          
#>  [7] "mtcars.csv.bz2"       "mtcars.csv.gz"       
#>  [9] "mtcars.csv.xz"        "mtcars.csv.zip"      

# Get path to one example
vroom_example("mtcars.csv")
#> [1] "/home/runner/work/_temp/Library/vroom/extdata/mtcars.csv"
```
