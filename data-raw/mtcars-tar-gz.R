withr::local_dir("inst/extdata/")

target <- "mtcars.csv.tar.gz"
# if target exists, nuke it first
if (file.exists(target)) {
  unlink(target)
}

tar(target, "mtcars.csv", compression = "gzip")

# check that the contents look as expected
untar(target, list = TRUE)

withr::deferred_run()
