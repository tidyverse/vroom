readidx
================

``` r
library(readidx)
```

readidx (name to be changed in the future) is an experiment for a future
version of readr (2.0), or a possible extension package.

It stems from the observation that IO is not the bottle neck in parsing
delimited datasets, rather (re)-allocating memory and parsing the values
into R data types (particularly for characters) takes the bulk of the
time.

It relies on the Altrep framework available in R 3.5 to provide lazy /
delayed parsing of values in delimited files.

## How it works

The initial reading of the file simply records the locations of each
individual record, the actual values are not read into R. Altrep vectors
are created for each column in the data which hold a pointer to the
index and the memory mapped file. When these vectors are indexed the
value is read from the memory mapping.

This means initial reading is extremely fast, in the example below it is
~ 1/4 the time of the multi-threaded `data.table::fread()`. Sampling
operations are likewise extremely fast, as only the data actually
included in the sample is read. This means things like the tibble print
method, calling `head()`, `tail()` `x[sample(), ]` etc. have very low
overhead.

Filtering also can be fast, only the columns included in the filter
itself have to be fully read across the entire dataset, only the
filtered rows need to be read from the remaining columns.

(*N.B. currently the dplyr implementation materializes the all numeric
vectors when using `filter()` or `sample_n()`, so these cases are not as
fast as they could otherwise be*).

This approach also allows you to work with data that is larger than
memory. As long as you are careful to avoid materializing the entire
dataset at once it can be efficiently queried and subset.

Once a particular vector is fully materialized the speed for all
subsequent operations should be identical to a normal R vector.

There is also lots of possible speed improvements available. The indexer
could be highly parallelized, as it does not rely on R data structures
at all. The index could also be stored on disk, which would make
re-reading the file at a later time basically instantaneous.
Materializing non-character vectors could also be parallelized.

## Dataset

The dataset used in these benchmarks is from FOIA/FOILed Taxi Trip Data
from the NYC Taxi and Limousine Commission 2013, originally posted at
<http://chriswhong.com/open-data/foil_nyc_taxi/>. It is also hosted on
[archive.org](https://archive.org/details/nycTaxiTripData2013).

The first table trip\_fare\_1.csv was converted to tsv and saved as
trip\_fare\_1.tsv, It is 1.55G in size.

    #> Observations: 14,776,615
    #> Variables: 11
    #> $ medallion       <chr> "89D227B655E5C82AECF13C3F540D4CF4", "0BD7C8F5B...
    #> $ hack_license    <chr> "BA96DE419E711691B9445D6A6307C170", "9FD8F69F0...
    #> $ vendor_id       <chr> "CMT", "CMT", "CMT", "CMT", "CMT", "CMT", "CMT...
    #> $ pickup_datetime <chr> "2013-01-01 15:11:48", "2013-01-06 00:18:35", ...
    #> $ payment_type    <chr> "CSH", "CSH", "CSH", "CSH", "CSH", "CSH", "CSH...
    #> $ fare_amount     <dbl> 6.5, 6.0, 5.5, 5.0, 9.5, 9.5, 6.0, 34.0, 5.5, ...
    #> $ surcharge       <dbl> 0.0, 0.5, 1.0, 0.5, 0.5, 0.0, 0.0, 0.0, 1.0, 0...
    #> $ mta_tax         <dbl> 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0...
    #> $ tip_amount      <dbl> 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0...
    #> $ tolls_amount    <dbl> 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 4.8, 0.0, 0...
    #> $ total_amount    <dbl> 7.0, 7.0, 7.0, 6.0, 10.5, 10.0, 6.5, 39.3, 7.0...

## Benchmarks

The benchmark `base` uses `readidx` with base functions for subsetting.
`dplyr` uses `readidx` to read the file and dplyr functions to subset.
`data.table` uses `fread()` to read the file and `data.table` functions
to subset and `readr` uses `readr` to read the file and `dplyr` to
subset.

The following operations are performed.

  - The data is read
  - `print()`
  - `head()`
  - `tail()`
  - Sampling 100 random rows
  - Filtering for “UNK” payment, this is 6434 rows (0.0435% of total).

<!-- end list -->

``` r
base <- function(file) {
  library(readidx)
  list(
    bench::system_time(x <- read_tsv(file)),
    bench::system_time(print(x)),
    bench::system_time(head(x)),
    bench::system_time(tail(x)),
    bench::system_time(x[sample(NROW(x), 100), ]),
    bench::system_time(x[x$payment_type == "UNK", ])
  )
}

dplyr <- function(file) {
  library(readidx)
  library(dplyr)
  list(
    bench::system_time(x <- read_tsv(file)),
    bench::system_time(print(x)),
    bench::system_time(head(x)),
    bench::system_time(tail(x)),
    bench::system_time(sample_n(x, 100)),
    bench::system_time(filter(x, payment_type == "UNK"))
  )
}

data.table <- function(file) {
  library(data.table)
  list(
    bench::system_time(x <- data.table::fread(file)),
    bench::system_time(print(x)),
    bench::system_time(head(x)),
    bench::system_time(tail(x)),
    bench::system_time(x[sample(NROW(x), 100), ]),
    bench::system_time(x[x$payment_type == "UNK", ])
  )
}

readr <- function(file) {
  library(readr)
  library(dplyr)
  list(
    bench::system_time(x <- read_tsv(file)),
    bench::system_time(print(x)),
    bench::system_time(head(x)),
    bench::system_time(tail(x)),
    bench::system_time(sample_n(x, 100)),
    bench::system_time(filter(x, payment_type == "UNK"))
  )
}

times <- list(
  base = callr::r(base, list(file = here::here("trip_fare_1.tsv"))),
  dplyr = callr::r(dplyr, list(file = here::here("trip_fare_1.tsv"))),
  data.table = callr::r(data.table, list(file = here::here("trip_fare_1.tsv"))),
  readr = callr::r(readr, list(file = here::here("trip_fare_1.tsv")))
)
```

<!--html_preserve-->

<style>html {
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Helvetica Neue', 'Fira Sans', 'Droid Sans', Arial, sans-serif;
}

#xtwfmpladv .gt_table {
  display: table;
  border-collapse: collapse;
  margin-left: auto;
  margin-right: auto;
  color: #000000;
  font-size: 16px;
  background-color: #FFFFFF;
  /* table.background.color */
  width: auto;
  /* table.width */
  border-top-style: solid;
  /* table.border.top.style */
  border-top-width: 2px;
  /* table.border.top.width */
  border-top-color: #A8A8A8;
  /* table.border.top.color */
}

#xtwfmpladv .gt_heading {
  background-color: #FFFFFF;
  /* heading.background.color */
  border-bottom-color: #FFFFFF;
}

#xtwfmpladv .gt_title {
  color: #000000;
  font-size: 125%;
  /* heading.title.font.size */
  padding-top: 4px;
  /* heading.top.padding */
  padding-bottom: 1px;
  border-bottom-color: #FFFFFF;
  border-bottom-width: 0;
}

#xtwfmpladv .gt_subtitle {
  color: #000000;
  font-size: 85%;
  /* heading.subtitle.font.size */
  padding-top: 1px;
  padding-bottom: 4px;
  /* heading.bottom.padding */
  border-top-color: #FFFFFF;
  border-top-width: 0;
}

#xtwfmpladv .gt_bottom_border {
  border-bottom-style: solid;
  /* heading.border.bottom.style */
  border-bottom-width: 2px;
  /* heading.border.bottom.width */
  border-bottom-color: #A8A8A8;
  /* heading.border.bottom.color */
}

#xtwfmpladv .gt_column_spanner {
  border-bottom-style: solid;
  border-bottom-width: 2px;
  border-bottom-color: #A8A8A8;
  padding-top: 4px;
  padding-bottom: 4px;
}

#xtwfmpladv .gt_col_heading {
  color: #000000;
  background-color: #FFFFFF;
  /* column_labels.background.color */
  font-size: 16px;
  /* column_labels.font.size */
  font-weight: initial;
  /* column_labels.font.weight */
  vertical-align: middle;
  padding: 10px;
  margin: 10px;
}

#xtwfmpladv .gt_sep_right {
  border-right: 5px solid #FFFFFF;
}

#xtwfmpladv .gt_group_heading {
  padding: 8px;
  color: #000000;
  background-color: #FFFFFF;
  /* stub_group.background.color */
  font-size: 16px;
  /* stub_group.font.size */
  font-weight: initial;
  /* stub_group.font.weight */
  border-top-style: solid;
  /* stub_group.border.top.style */
  border-top-width: 2px;
  /* stub_group.border.top.width */
  border-top-color: #A8A8A8;
  /* stub_group.border.top.color */
  border-bottom-style: solid;
  /* stub_group.border.bottom.style */
  border-bottom-width: 2px;
  /* stub_group.border.bottom.width */
  border-bottom-color: #A8A8A8;
  /* stub_group.border.bottom.color */
  vertical-align: middle;
}

#xtwfmpladv .gt_empty_group_heading {
  padding: 0.5px;
  color: #000000;
  background-color: #FFFFFF;
  /* stub_group.background.color */
  font-size: 16px;
  /* stub_group.font.size */
  font-weight: initial;
  /* stub_group.font.weight */
  border-top-style: solid;
  /* stub_group.border.top.style */
  border-top-width: 2px;
  /* stub_group.border.top.width */
  border-top-color: #A8A8A8;
  /* stub_group.border.top.color */
  border-bottom-style: solid;
  /* stub_group.border.bottom.style */
  border-bottom-width: 2px;
  /* stub_group.border.bottom.width */
  border-bottom-color: #A8A8A8;
  /* stub_group.border.bottom.color */
  vertical-align: middle;
}

#xtwfmpladv .gt_striped {
  background-color: #f2f2f2;
}

#xtwfmpladv .gt_row {
  padding: 10px;
  /* row.padding */
  margin: 10px;
  vertical-align: middle;
}

#xtwfmpladv .gt_stub {
  border-right-style: solid;
  border-right-width: 2px;
  border-right-color: #A8A8A8;
  padding-left: 12px;
}

#xtwfmpladv .gt_stub.gt_row {
  background-color: #FFFFFF;
}

#xtwfmpladv .gt_summary_row {
  background-color: #FFFFFF;
  /* summary_row.background.color */
  padding: 6px;
  /* summary_row.padding */
  text-transform: inherit;
  /* summary_row.text_transform */
}

#xtwfmpladv .gt_first_summary_row {
  border-top-style: solid;
  border-top-width: 2px;
  border-top-color: #A8A8A8;
}

#xtwfmpladv .gt_table_body {
  border-top-style: solid;
  /* field.border.top.style */
  border-top-width: 2px;
  /* field.border.top.width */
  border-top-color: #A8A8A8;
  /* field.border.top.color */
  border-bottom-style: solid;
  /* field.border.bottom.style */
  border-bottom-width: 2px;
  /* field.border.bottom.width */
  border-bottom-color: #A8A8A8;
  /* field.border.bottom.color */
}

#xtwfmpladv .gt_footnote {
  font-size: 90%;
  /* footnote.font.size */
  padding: 4px;
  /* footnote.padding */
}

#xtwfmpladv .gt_sourcenote {
  font-size: 90%;
  /* sourcenote.font.size */
  padding: 4px;
  /* sourcenote.padding */
}

#xtwfmpladv .gt_center {
  text-align: center;
}

#xtwfmpladv .gt_left {
  text-align: left;
}

#xtwfmpladv .gt_right {
  text-align: right;
  font-variant-numeric: tabular-nums;
}

#xtwfmpladv .gt_font_normal {
  font-weight: normal;
}

#xtwfmpladv .gt_font_bold {
  font-weight: bold;
}

#xtwfmpladv .gt_font_italic {
  font-style: italic;
}

#xtwfmpladv .gt_super {
  font-size: 65%;
}

#xtwfmpladv .gt_footnote_glyph {
  font-style: italic;
  font-size: 65%;
}
</style>

<div id="xtwfmpladv" style="overflow-x:auto;">

<!--gt table start-->

<table class="gt_table">

<thead>

<tr>

<th class="gt_heading gt_title gt_font_normal gt_center" colspan="2">

Real time to read the
file

</th>

</tr>

<tr>

<th class="gt_heading gt_subtitle gt_font_normal gt_center gt_bottom_border" colspan="2">

(base and dplyr both use `readidx`, so are equivalent)

</th>

</tr>

</thead>

<tr>

<th class="gt_col_heading gt_center" rowspan="1" colspan="1">

package

</th>

<th class="gt_col_heading gt_right" rowspan="1" colspan="1">

time

</th>

</tr>

<tbody class="gt_table_body">

<tr>

<td class="gt_row gt_center">

base

</td>

<td class="gt_row gt_right">

2.56

</td>

</tr>

<tr>

<td class="gt_row gt_center gt_striped">

dplyr

</td>

<td class="gt_row gt_right gt_striped">

2.43

</td>

</tr>

<tr>

<td class="gt_row gt_center">

data.table

</td>

<td class="gt_row gt_right">

19.62

</td>

</tr>

<tr>

<td class="gt_row gt_center gt_striped">

readr

</td>

<td class="gt_row gt_right gt_striped">

27.06

</td>

</tr>

</tbody>

</table>

<!--gt table end-->

</div>

<!--/html_preserve-->

<!--html_preserve-->

<style>html {
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Helvetica Neue', 'Fira Sans', 'Droid Sans', Arial, sans-serif;
}

#xidjzyikat .gt_table {
  display: table;
  border-collapse: collapse;
  margin-left: auto;
  margin-right: auto;
  color: #000000;
  font-size: 16px;
  background-color: #FFFFFF;
  /* table.background.color */
  width: auto;
  /* table.width */
  border-top-style: solid;
  /* table.border.top.style */
  border-top-width: 2px;
  /* table.border.top.width */
  border-top-color: #A8A8A8;
  /* table.border.top.color */
}

#xidjzyikat .gt_heading {
  background-color: #FFFFFF;
  /* heading.background.color */
  border-bottom-color: #FFFFFF;
}

#xidjzyikat .gt_title {
  color: #000000;
  font-size: 125%;
  /* heading.title.font.size */
  padding-top: 4px;
  /* heading.top.padding */
  padding-bottom: 1px;
  border-bottom-color: #FFFFFF;
  border-bottom-width: 0;
}

#xidjzyikat .gt_subtitle {
  color: #000000;
  font-size: 85%;
  /* heading.subtitle.font.size */
  padding-top: 1px;
  padding-bottom: 4px;
  /* heading.bottom.padding */
  border-top-color: #FFFFFF;
  border-top-width: 0;
}

#xidjzyikat .gt_bottom_border {
  border-bottom-style: solid;
  /* heading.border.bottom.style */
  border-bottom-width: 2px;
  /* heading.border.bottom.width */
  border-bottom-color: #A8A8A8;
  /* heading.border.bottom.color */
}

#xidjzyikat .gt_column_spanner {
  border-bottom-style: solid;
  border-bottom-width: 2px;
  border-bottom-color: #A8A8A8;
  padding-top: 4px;
  padding-bottom: 4px;
}

#xidjzyikat .gt_col_heading {
  color: #000000;
  background-color: #FFFFFF;
  /* column_labels.background.color */
  font-size: 16px;
  /* column_labels.font.size */
  font-weight: initial;
  /* column_labels.font.weight */
  vertical-align: middle;
  padding: 10px;
  margin: 10px;
}

#xidjzyikat .gt_sep_right {
  border-right: 5px solid #FFFFFF;
}

#xidjzyikat .gt_group_heading {
  padding: 8px;
  color: #000000;
  background-color: #FFFFFF;
  /* stub_group.background.color */
  font-size: 16px;
  /* stub_group.font.size */
  font-weight: initial;
  /* stub_group.font.weight */
  border-top-style: solid;
  /* stub_group.border.top.style */
  border-top-width: 2px;
  /* stub_group.border.top.width */
  border-top-color: #A8A8A8;
  /* stub_group.border.top.color */
  border-bottom-style: solid;
  /* stub_group.border.bottom.style */
  border-bottom-width: 2px;
  /* stub_group.border.bottom.width */
  border-bottom-color: #A8A8A8;
  /* stub_group.border.bottom.color */
  vertical-align: middle;
}

#xidjzyikat .gt_empty_group_heading {
  padding: 0.5px;
  color: #000000;
  background-color: #FFFFFF;
  /* stub_group.background.color */
  font-size: 16px;
  /* stub_group.font.size */
  font-weight: initial;
  /* stub_group.font.weight */
  border-top-style: solid;
  /* stub_group.border.top.style */
  border-top-width: 2px;
  /* stub_group.border.top.width */
  border-top-color: #A8A8A8;
  /* stub_group.border.top.color */
  border-bottom-style: solid;
  /* stub_group.border.bottom.style */
  border-bottom-width: 2px;
  /* stub_group.border.bottom.width */
  border-bottom-color: #A8A8A8;
  /* stub_group.border.bottom.color */
  vertical-align: middle;
}

#xidjzyikat .gt_striped {
  background-color: #f2f2f2;
}

#xidjzyikat .gt_row {
  padding: 10px;
  /* row.padding */
  margin: 10px;
  vertical-align: middle;
}

#xidjzyikat .gt_stub {
  border-right-style: solid;
  border-right-width: 2px;
  border-right-color: #A8A8A8;
  padding-left: 12px;
}

#xidjzyikat .gt_stub.gt_row {
  background-color: #FFFFFF;
}

#xidjzyikat .gt_summary_row {
  background-color: #FFFFFF;
  /* summary_row.background.color */
  padding: 6px;
  /* summary_row.padding */
  text-transform: inherit;
  /* summary_row.text_transform */
}

#xidjzyikat .gt_first_summary_row {
  border-top-style: solid;
  border-top-width: 2px;
  border-top-color: #A8A8A8;
}

#xidjzyikat .gt_table_body {
  border-top-style: solid;
  /* field.border.top.style */
  border-top-width: 2px;
  /* field.border.top.width */
  border-top-color: #A8A8A8;
  /* field.border.top.color */
  border-bottom-style: solid;
  /* field.border.bottom.style */
  border-bottom-width: 2px;
  /* field.border.bottom.width */
  border-bottom-color: #A8A8A8;
  /* field.border.bottom.color */
}

#xidjzyikat .gt_footnote {
  font-size: 90%;
  /* footnote.font.size */
  padding: 4px;
  /* footnote.padding */
}

#xidjzyikat .gt_sourcenote {
  font-size: 90%;
  /* sourcenote.font.size */
  padding: 4px;
  /* sourcenote.padding */
}

#xidjzyikat .gt_center {
  text-align: center;
}

#xidjzyikat .gt_left {
  text-align: left;
}

#xidjzyikat .gt_right {
  text-align: right;
  font-variant-numeric: tabular-nums;
}

#xidjzyikat .gt_font_normal {
  font-weight: normal;
}

#xidjzyikat .gt_font_bold {
  font-weight: bold;
}

#xidjzyikat .gt_font_italic {
  font-style: italic;
}

#xidjzyikat .gt_super {
  font-size: 65%;
}

#xidjzyikat .gt_footnote_glyph {
  font-style: italic;
  font-size: 65%;
}
</style>

<div id="xidjzyikat" style="overflow-x:auto;">

<!--gt table start-->

<table class="gt_table">

<thead>

<tr>

<th class="gt_heading gt_title gt_font_normal gt_center" colspan="2">

Total time for all
operations

</th>

</tr>

<tr>

<th class="gt_heading gt_subtitle gt_font_normal gt_center gt_bottom_border" colspan="2">

</th>

</tr>

</thead>

<tr>

<th class="gt_col_heading gt_center" rowspan="1" colspan="1">

package

</th>

<th class="gt_col_heading gt_right" rowspan="1" colspan="1">

time

</th>

</tr>

<tbody class="gt_table_body">

<tr>

<td class="gt_row gt_center">

base

</td>

<td class="gt_row gt_right">

4.74

</td>

</tr>

<tr>

<td class="gt_row gt_center gt_striped">

dplyr

</td>

<td class="gt_row gt_right gt_striped">

7.33

</td>

</tr>

<tr>

<td class="gt_row gt_center">

data.table

</td>

<td class="gt_row gt_right">

19.79

</td>

</tr>

<tr>

<td class="gt_row gt_center gt_striped">

readr

</td>

<td class="gt_row gt_right gt_striped">

27.44

</td>

</tr>

</tbody>

</table>

<!--gt table end-->

</div>

<!--/html_preserve-->

Graph of timings, note because data.table operations use multiple cores
the processor time is often much higher than the real time.

``` r
library(ggplot2)
tm_df %>%
  ggplot() +
  geom_segment(y = 0, aes(x = package, xend = package, yend = time, alpha = type), color = "grey50") +
    geom_point(aes(x = package, y = time, color = type)) +
    facet_wrap(vars(op), scales = "free") +
    bench::scale_y_bench_time(base = NULL) +
    theme(legend.position = "bottom")
```

![](benchmarks_files/figure-gfm/unnamed-chunk-7-1.png)<!-- -->

``` r
sessioninfo::package_info(c("readidx", "readr", "dplyr", "data.table"), dependencies = FALSE)
#>  package    * version    date       lib source        
#>  data.table   1.11.8     2018-09-30 [1] CRAN (R 3.5.0)
#>  dplyr      * 0.7.8      2018-11-10 [1] CRAN (R 3.5.0)
#>  readidx    * 0.0.0.9000 2018-12-27 [1] local         
#>  readr        1.3.1      2018-12-21 [1] CRAN (R 3.5.0)
#> 
#> [1] /Users/jhester/Library/R/3.5/library
#> [2] /Library/Frameworks/R.framework/Versions/3.5/Resources/library
```
