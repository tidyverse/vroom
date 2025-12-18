# Package index

## Read rectangular files

These functions parse rectangular files (like csv or fixed-width format)
into tibbles. They specify the overall structure of the file, and how
each line is divided up into fields.

- [`vroom()`](https://vroom.tidyverse.org/dev/reference/vroom.md) : Read
  a delimited file into a tibble
- [`vroom_fwf()`](https://vroom.tidyverse.org/dev/reference/vroom_fwf.md)
  [`fwf_empty()`](https://vroom.tidyverse.org/dev/reference/vroom_fwf.md)
  [`fwf_widths()`](https://vroom.tidyverse.org/dev/reference/vroom_fwf.md)
  [`fwf_positions()`](https://vroom.tidyverse.org/dev/reference/vroom_fwf.md)
  [`fwf_cols()`](https://vroom.tidyverse.org/dev/reference/vroom_fwf.md)
  : Read a fixed-width file into a tibble
- [`problems()`](https://vroom.tidyverse.org/dev/reference/problems.md)
  : Retrieve parsing problems

## Write rectangular files

These functions write data frames to disk, or to convert them to
in-memory strings.

- [`vroom_write()`](https://vroom.tidyverse.org/dev/reference/vroom_write.md)
  : Write a data frame to a delimited file
- [`vroom_write_lines()`](https://vroom.tidyverse.org/dev/reference/vroom_write_lines.md)
  : Write lines to a file
- [`vroom_format()`](https://vroom.tidyverse.org/dev/reference/vroom_format.md)
  : Convert a data frame to a delimited string
- [`output_column()`](https://vroom.tidyverse.org/dev/reference/output_column.md)
  : Preprocess column for output

## Column specification

The column specification describes how each column is parsed from a
character vector in to a more specific data type. vroom does make an
educated guess about the type of each column, but you’ll need override
those guesses when it gets them wrong.

- [`as.col_spec()`](https://vroom.tidyverse.org/dev/reference/as.col_spec.md)
  : Coerce to a column specification
- [`cols()`](https://vroom.tidyverse.org/dev/reference/cols.md)
  [`cols_only()`](https://vroom.tidyverse.org/dev/reference/cols.md)
  [`col_logical()`](https://vroom.tidyverse.org/dev/reference/cols.md)
  [`col_integer()`](https://vroom.tidyverse.org/dev/reference/cols.md)
  [`col_big_integer()`](https://vroom.tidyverse.org/dev/reference/cols.md)
  [`col_double()`](https://vroom.tidyverse.org/dev/reference/cols.md)
  [`col_character()`](https://vroom.tidyverse.org/dev/reference/cols.md)
  [`col_skip()`](https://vroom.tidyverse.org/dev/reference/cols.md)
  [`col_number()`](https://vroom.tidyverse.org/dev/reference/cols.md)
  [`col_guess()`](https://vroom.tidyverse.org/dev/reference/cols.md)
  [`col_factor()`](https://vroom.tidyverse.org/dev/reference/cols.md)
  [`col_datetime()`](https://vroom.tidyverse.org/dev/reference/cols.md)
  [`col_date()`](https://vroom.tidyverse.org/dev/reference/cols.md)
  [`col_time()`](https://vroom.tidyverse.org/dev/reference/cols.md) :
  Create column specification
- [`cols_condense()`](https://vroom.tidyverse.org/dev/reference/spec.md)
  [`spec()`](https://vroom.tidyverse.org/dev/reference/spec.md) :
  Examine the column specifications for a data frame
- [`guess_type()`](https://vroom.tidyverse.org/dev/reference/guess_type.md)
  : Guess the type of a vector

## Locale controls

The “locale” controls all options that vary from country-to-country or
language-to-language. This includes things like the character used as
the decimal mark, the names of days of the week, and the encoding. See
`vignette("locales")` for more details.

- [`locale()`](https://vroom.tidyverse.org/dev/reference/locale.md)
  [`default_locale()`](https://vroom.tidyverse.org/dev/reference/locale.md)
  : Create locales
- [`date_names()`](https://vroom.tidyverse.org/dev/reference/date_names.md)
  [`date_names_lang()`](https://vroom.tidyverse.org/dev/reference/date_names.md)
  [`date_names_langs()`](https://vroom.tidyverse.org/dev/reference/date_names.md)
  : Create or retrieve date names

## Data generation

vroom provides a number of functions to generate datasets based on a
column specification. These are mainly used for development and
benchmarking, but can also be useful for reproducing bugs without
requiring the original dataset.

- [`gen_tbl()`](https://vroom.tidyverse.org/dev/reference/gen_tbl.md) :
  Generate a random tibble
- [`gen_character()`](https://vroom.tidyverse.org/dev/reference/generators.md)
  [`gen_double()`](https://vroom.tidyverse.org/dev/reference/generators.md)
  [`gen_number()`](https://vroom.tidyverse.org/dev/reference/generators.md)
  [`gen_integer()`](https://vroom.tidyverse.org/dev/reference/generators.md)
  [`gen_factor()`](https://vroom.tidyverse.org/dev/reference/generators.md)
  [`gen_time()`](https://vroom.tidyverse.org/dev/reference/generators.md)
  [`gen_date()`](https://vroom.tidyverse.org/dev/reference/generators.md)
  [`gen_datetime()`](https://vroom.tidyverse.org/dev/reference/generators.md)
  [`gen_logical()`](https://vroom.tidyverse.org/dev/reference/generators.md)
  [`gen_name()`](https://vroom.tidyverse.org/dev/reference/generators.md)
  : Generate individual vectors of the types supported by vroom

## Misc tools

These functions are used as helpers for other functions, or to inspect
objects.

- [`vroom_lines()`](https://vroom.tidyverse.org/dev/reference/vroom_lines.md)
  : Read lines from a file
- [`vroom_altrep()`](https://vroom.tidyverse.org/dev/reference/vroom_altrep.md)
  : Show which column types are using Altrep
- [`vroom_example()`](https://vroom.tidyverse.org/dev/reference/vroom_example.md)
  [`vroom_examples()`](https://vroom.tidyverse.org/dev/reference/vroom_example.md)
  : Get path to vroom examples
- [`vroom_progress()`](https://vroom.tidyverse.org/dev/reference/vroom_progress.md)
  : Determine whether progress bars should be shown
- [`vroom_str()`](https://vroom.tidyverse.org/dev/reference/vroom_str.md)
  : Structure of objects
