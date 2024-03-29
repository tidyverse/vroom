% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/vroom_lines.R
\name{vroom_lines}
\alias{vroom_lines}
\title{Read lines from a file}
\usage{
vroom_lines(
  file,
  n_max = Inf,
  skip = 0,
  na = character(),
  skip_empty_rows = FALSE,
  locale = default_locale(),
  altrep = TRUE,
  altrep_opts = deprecated(),
  num_threads = vroom_threads(),
  progress = vroom_progress()
)
}
\arguments{
\item{file}{Either a path to a file, a connection, or literal data (either a
single string or a raw vector). \code{file} can also be a character vector
containing multiple filepaths or a list containing multiple connections.

Files ending in \code{.gz}, \code{.bz2}, \code{.xz}, or \code{.zip} will be automatically
uncompressed. Files starting with \verb{http://}, \verb{https://}, \verb{ftp://}, or
\verb{ftps://} will be automatically downloaded. Remote gz files can also be
automatically downloaded and decompressed.

Literal data is most useful for examples and tests. To be recognised as
literal data, wrap the input with \code{I()}.}

\item{n_max}{Maximum number of lines to read.}

\item{skip}{Number of lines to skip before reading data. If \code{comment} is
supplied any commented lines are ignored \emph{after} skipping.}

\item{na}{Character vector of strings to interpret as missing values. Set this
option to \code{character()} to indicate no missing values.}

\item{skip_empty_rows}{Should blank rows be ignored altogether? i.e. If this
option is \code{TRUE} then blank rows will not be represented at all.  If it is
\code{FALSE} then they will be represented by \code{NA} values in all the columns.}

\item{locale}{The locale controls defaults that vary from place to place.
The default locale is US-centric (like R), but you can use
\code{\link[=locale]{locale()}} to create your own locale that controls things like
the default time zone, encoding, decimal mark, big mark, and day/month
names.}

\item{altrep}{Control which column types use Altrep representations,
either a character vector of types, \code{TRUE} or \code{FALSE}. See
\code{\link[=vroom_altrep]{vroom_altrep()}} for for full details.}

\item{altrep_opts}{\Sexpr[results=rd, stage=render]{lifecycle::badge("deprecated")}}

\item{num_threads}{Number of threads to use when reading and materializing
vectors. If your data contains newlines within fields the parser will
automatically be forced to use a single thread only.}

\item{progress}{Display a progress bar? By default it will only display
in an interactive session and not while knitting a document. The automatic
progress bar can be disabled by setting option \code{readr.show_progress} to
\code{FALSE}.}
}
\description{
\code{vroom_lines()} is similar to \code{readLines()}, however it reads the lines
lazily like \code{\link[=vroom]{vroom()}}, so operations like \code{length()}, \code{head()}, \code{tail()} and \code{sample()}
can be done much more efficiently without reading all the data into R.
}
\examples{
lines <- vroom_lines(vroom_example("mtcars.csv"))

length(lines)
head(lines, n = 2)
tail(lines, n = 2)
sample(lines, size = 2)
}
