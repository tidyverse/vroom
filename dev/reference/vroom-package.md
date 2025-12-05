# vroom: Read and Write Rectangular Text Data Quickly

The goal of 'vroom' is to read and write data (like 'csv', 'tsv' and
'fwf') quickly. When reading it uses a quick initial indexing step, then
reads the values lazily , so only the data you actually use needs to be
read. The writer formats the data in parallel and writes to disk
asynchronously from formatting.

## See also

Useful links:

- <https://vroom.tidyverse.org>

- <https://github.com/tidyverse/vroom>

- Report bugs at <https://github.com/tidyverse/vroom/issues>

## Author

**Maintainer**: Jennifer Bryan <jenny@posit.co>
([ORCID](https://orcid.org/0000-0002-6983-2759))

Authors:

- Jim Hester ([ORCID](https://orcid.org/0000-0002-2739-7082))

- Hadley Wickham <hadley@posit.co>
  ([ORCID](https://orcid.org/0000-0003-4757-117X))

Other contributors:

- Shelby Bearrows \[contributor\]

- https://github.com/mandreyel/ (mio library) \[copyright holder\]

- Jukka Jylänki (grisu3 implementation) \[copyright holder\]

- Mikkel Jørgensen (grisu3 implementation) \[copyright holder\]

- Posit Software, PBC ([ROR](https://ror.org/03wc8by49)) \[copyright
  holder, funder\]
