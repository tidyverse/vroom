# vroom errors if numbers of columns are inconsistent

    Code
      vroom::vroom(files, col_types = list())
    Condition
      Error:
      ! Files must all have 2 columns:
      i File 2 has 3 columns.

# vroom errors if column names are inconsistent

    Code
      vroom::vroom(files, col_types = list())
    Condition
      Error:
      ! Files must have consistent column names:
      * File 1 column 1 is: A
      * File 2 column 1 is: C

