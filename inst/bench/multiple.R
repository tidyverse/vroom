files <- c("~/data/trip_fare_1.csv", "~/data/trip_fare_2.csv", "~/data/trip_fare_3.csv")

vroom_base <- function(files) {
  library(vroom)
  list(
    bench::system_time(x <- vroom(files, quote = "", escape_double = FALSE, na = character())),
    bench::system_time(print(x)),
    bench::system_time(head(x)),
    bench::system_time(tail(x)),
    bench::system_time(x[sample(NROW(x), 100), ]),
    bench::system_time(x[x$payment_type == "UNK", ])
  )
}
