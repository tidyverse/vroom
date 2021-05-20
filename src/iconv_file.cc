#include <cerrno>
#include <cpp11/R.hpp>

#include "R_ext/Riconv.h"

#include "connection.h"

#define BUFSIZ 1024

// Adapted from
// https://www.gnu.org/software/libc/manual/html_node/iconv-Examples.html

[[cpp11::register]] size_t convert_connection(
    SEXP in_con, SEXP out_con, std::string from, std::string to) {

  static auto close = cpp11::package("base")["close"];

  char inbuf[BUFSIZ];
  char wrbuf[BUFSIZ * 4];
  char* wrptr = wrbuf;
  size_t insize = 0;
  int result = 0;
  void* cd;

  cd = Riconv_open(to.c_str(), from.c_str());
  if (cd == (void*)-1) {
    /* Something went wrong.  */
    if (errno == EINVAL) {
      close(in_con);
      close(out_con);
      cpp11::stop("Can't convert from %s to %s", from.c_str(), to.c_str());
    } else {
      close(in_con);
      close(out_con);
      cpp11::stop("Iconv initialisation failed");
    }
  }

  size_t avail = BUFSIZ * 4;

  size_t bytes_wrote = 0;

  while (avail > 0) {
    size_t nread;
    size_t nconv;
    const char* inptr = inbuf;

    /* Read more input.  */
    nread = R_ReadConnection(in_con, inbuf + insize, sizeof(inbuf) - insize);
    if (nread == 0) {
      /* When we come here the file is completely read.
         This still could mean there are some unused
         characters in the inbuf. */
      result = -1;

      /* Now write out the byte sequence to get into the
         initial state if this is necessary.  */
      Riconv(cd, NULL, NULL, &wrptr, &avail);

      break;
    }
    insize += nread;

    /* Do the conversion.  */
    nconv = Riconv(cd, &inptr, &insize, &wrptr, &avail);
    if (nconv == (size_t)-1) {
      /* Not everything went right.  It might only be
         an unfinished byte sequence at the end of the
         buffer.  Or it is a real problem.  */
      if (errno == EINVAL) {
        /* This is harmless.  Simply move the unused
           bytes to the beginning of the buffer so that
           they can be used in the next round.  */
        memmove(inbuf, inptr, insize);
      } else {
        /* It is a real problem.  Maybe we ran out of
           space in the output buffer or we have invalid
           input.  */
        result = -1;
        close(in_con);
        close(out_con);
        cpp11::stop("iconv failed");
        break;
      }
    }

    R_WriteConnection(out_con, wrbuf, wrptr - wrbuf);
    bytes_wrote += wrptr - wrbuf;
    wrptr = wrbuf;
    avail = sizeof(wrbuf);
  }

  if (Riconv_close(cd) != 0) {
    close(in_con);
    close(out_con);
    cpp11::stop("Iconv closed failed");
  }

  close(in_con);
  close(out_con);

  return bytes_wrote;
}
