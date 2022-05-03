# Setup

# create the files however you'd prefer
# but they can't be temp files
destdir <- "../think_tank/testFiles/"

#brio::write_lines("a", file.path(destdir, "no-rows.tsv"))
#brio::write_lines(c("a","x"), file.path(destdir, "one-row.tsv"))

# Debugging

devtools::clean_dll()
devtools::load_all()


# also, when I'm in debug mode in VSCode
# if I have altrep = TRUE (the default) it doesn't segfault
# but this is only true when I'm using the debugger in VSCode
# I've also been setting num_threads to 1 to make it easier to debug
vroom::vroom(
    file.path(destdir, c("no-rows.tsv", "one-row.tsv")
  ), altrep = FALSE, num_threads = 1, delim = "\t")

#vroom::vroom(c(
#"../think_tank/testFiles/also-one-row.tsv",
#"../think_tank/testFiles/no-rows.tsv",
#"../think_tank/testFiles/another-one-row.tsv"), 
#  altrep = FALSE, num_threads = 1)