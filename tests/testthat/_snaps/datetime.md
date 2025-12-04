# malformed date / datetime formats cause R errors

    Code
      vroom(I("x\n6/28/2016"), delim = ",", col_types = list(x = col_date("%m/%/%Y")),
      altrep = FALSE)
    Condition
      Error:
      ! Unsupported format

---

    Code
      vroom(I("x\n6/28/2016"), delim = ",", col_types = list(x = col_datetime(
        "%m/%/%Y")), altrep = FALSE)
    Condition
      Error:
      ! Unsupported format

