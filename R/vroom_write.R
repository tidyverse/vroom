vroom_write <- function(df, out, lines = 100, num_threads = vroom_threads()) {
  vroom_write_(df, out, lines, num_threads)
}
