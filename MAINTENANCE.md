## Current state

My talk at UseR 2019 gives a good overview of the goals and high level design of the package (https://www.youtube.com/watch?v=RA9AjqZXxMU&t=10s)

Some of the code in vroom has grown gnarly over time as more and more edge cases have to be handled.

Particularly nasty bits are `col_spec_standardise()` and the code around `ingex_region()`, which can be fiddly with off by one errors.

It could probably stand to be re-factored and possibly simplified, if this is attempted the test cases should contain most of the known edge cases.
The test coverage is fortunately decent.

Particular points that tend to crop up is there are different code paths for the following things
- reading from normal files or connections
- line endings ('\r\n', '\r', or '\n')
- files ending with a trailing newline or not.
- Use of ALTREP or not

The test helper `test_vroom()` automatically tests the content with normal
files and connections and with ALTREP on or off, but does not do any tests for
line endings or trailing newlines.

Files without a trailing newline are automatically detected and always sent down the code that reads from a connection.

The only code path that is multi-threaded is normal files, connections are read asynchronously and written to a temporary file, which is then read as normal.

### Debugging

To compile with logging enabled you need to set `-DVROOM_LOG` in your `~/R/Makevars` and if you want to control the logging level you can set `-DSPDLOG_ACTIVE_LEVEL=SPDLOG_LEVEL_DEBUG`.
You also need to create the `logs` directory for the logs to be written to. They will write a `logs/index.idx` and a `logs/index_connection.idx` file respectively.
The file is appended to, not rewritten, so you would need to delete it if you want a new file after a run.
There is also `-DVROOM_USE_CONNECTIONS_API` to use the CRAN forbidden connections API directly, but the performance difference is generally the same, so it isn't really needed.

## Known outstanding issues

Currently the line numbers in `problems()` don't take into account skipped or commented lines, as we don't keep track of how many of those we have seen.
This would not be entirely trivial to do, as we would need to keep track of the locations of the skipped or commented lines in each chunk, so we could fix them up when assembling the problems data frame.
It is not clear to me that this trade off is worth the effort.
On the other hand reporting incorrect line numbers is also not very good.

https://github.com/r-lib/vroom/issues/295 tracks this issue

## Future directions

In general it is not clear to me how much time and effort should continue to go
into this in the future. Should we consider using arrow as our primary way to
read in CSV files rather than relying on maintaining this codebase?

The lazy reading is interesting approach in theory, but it is not clear to me
how many people it helps in practice. It also is effective for R primarily
because of how comparatively slow R is at constructing strings, due to its
reliance on the global string pool.

### Memory mapped index

[vroom-mmap-index](https://github.com/r-lib/vroom/compare/vroom-mmap-index?expand=1) has some experimental code for storing the index in a mmap file instead of in memory.
This seemed to work well with limited testing, actually being a bit faster in initial testing then the current approach, and would allow you to handle bigger files.
It would also allow you to potentially save the index once and re-use it.

The potential challenges are where to store the index by default, the API for
how to store it, and the exact incantation needed to grow the index dynamically
if needed, as we won't know what size we need until we are done parsing.

https://github.com/r-lib/vroom/issues/51#issue-414823372 tracks this issue.

### Using SIMD code for the indexing

[simdcsv](https://github.com/jimhester/simdcsv) is an experiment with writing a
CSV parser that uses SIMD approaches similar to
[simdjson](https://github.com/simdjson/simdjson) with some speculative parsing
ideas from another paper to figure out if you are in a quoted block or not when
doing multiple threads.

Benchmarks indicate this approach can be ~2x as fast as the current parser or so.

You would still then have to parse the data afterwards, it is possible there
are approaches that could make this faster than the current implementations as
well.

## Alternative handling of files without trailing newlines

There is a Stack overflow thread which suggests that if you mmap a file beyond the size the mmap is guaranteed to pad the remainder with 0's.
If this is true we could always do this and in that way handle the case when a
file doesn't end with a newline, which would simplify the implementation for
these files, and allow us to use multi-threads to read them.

However we would have to investigate if this behavior is actually true across platforms or not.

strcspn requires the data must end with a NUL, so if we cannot guarantee that this approach won't work.

https://github.com/r-lib/vroom/issues/357 tracks this issue
