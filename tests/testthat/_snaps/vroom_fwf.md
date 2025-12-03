# fwf_cols errors when arguments have different shapes

    Code
      fwf_cols(a = 10, b = c(11, 15))
    Condition
      Error in `fwf_cols()`:
      ! All inputs must have the same shape.
      x Found inputs with different lengths: 1 and 2.
      i Provide either single values (widths) or pairs of values (positions).

---

    Code
      fwf_cols(a = 1:3, b = 4:5)
    Condition
      Error in `fwf_cols()`:
      ! All inputs must have the same shape.
      x Found inputs with different lengths: 3 and 2.
      i Provide either single values (widths) or pairs of values (positions).

# fwf_cols errors with invalid number of values

    Code
      fwf_cols(a = 1:4, b = 5:8)
    Condition
      Error in `fwf_cols()`:
      ! All inputs must be either a single value or a pair of values.
      x The provided inputs each have length 4.
      i Single values specify column widths: `fwf_cols(a = 10, b = 5)`.
      i Pairs of values specify start and end positions: `fwf_cols(a = c(1, 10), b = c(11, 15))`.

---

    Code
      fwf_cols(a = c(), b = c())
    Condition
      Error in `fwf_cols()`:
      ! All inputs must be either a single value or a pair of values.
      x The provided inputs each have length 0.
      i Single values specify column widths: `fwf_cols(a = 10, b = 5)`.
      i Pairs of values specify start and end positions: `fwf_cols(a = c(1, 10), b = c(11, 15))`.

# Errors if begin is greater than end

    Code
      vroom_fwf(I("1  2  3\n"), positions, col_types = list())
    Condition
      Error in `verify_fwf_positions()`:
      ! `begin` cannot be greater than `end`.
      x Problem with column: "bar".

