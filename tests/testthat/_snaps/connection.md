# vroom errors when the connection buffer is too small

    Code
      vroom(file(vroom_example("mtcars.csv")), col_types = list())
    Condition
      Error:
      ! The size of the connection buffer (32) was not large enough
      to fit a complete line:
        * Increase it by setting `Sys.setenv("VROOM_CONNECTION_SIZE")`

