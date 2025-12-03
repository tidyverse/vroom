# locale() errors when decimal_mark equals grouping_mark

    Code
      locale(decimal_mark = ".", grouping_mark = ".")
    Condition
      Error in `locale()`:
      ! `decimal_mark` and `grouping_mark` must be different.
      i Both were specified as ".".

# locale() validates timezone

    Code
      locale(tz = "Invalid/Timezone")
    Condition
      Error in `locale()`:
      ! Unknown timezone: "Invalid/Timezone".

# locale() can consult and validate system time zone

    Code
      locale(tz = "")
    Condition
      Error in `locale()`:
      ! Unknown system timezone: "foo".

