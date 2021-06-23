#include <cerrno>
#include <cpp11/R.hpp>

#include "R_ext/Riconv.h"

#include "connection.h"

#define VROOM_BUFSIZ 1024

// Adapted from
// https://www.gnu.org/software/libc/manual/html_node/iconv-Examples.html

[[cpp11::register]] size_t convert_connection(
    SEXP in_con, SEXP out_con, const std::string& from, const std::string& to) {

  static auto isOpen = cpp11::package("base")["isOpen"];
  static auto open = cpp11::package("base")["open"];
  static auto close = cpp11::package("base")["close"];

  char inbuf[VROOM_BUFSIZ];
  char wrbuf[VROOM_BUFSIZ * 4];
  char* wrptr = wrbuf;
  size_t insize = 0;
  void* cd;

  bool should_close_in = !isOpen(in_con);
  bool should_close_out = !isOpen(out_con);

  if (should_close_in) {
    open(in_con, "rb");
  }

  if (should_close_out) {
    open(out_con, "wb");
  }

  cd = Riconv_open(to.c_str(), from.c_str());
  if (cd == (void*)-1) {
    /* Something went wrong.  */
    if (errno == EINVAL) {
      if (should_close_in) {
        close(in_con);
      }
      if (should_close_out) {
        close(out_con);
      }
      cpp11::stop("Can't convert from %s to %s", from.c_str(), to.c_str());
    } else {
      if (should_close_in) {
        close(in_con);
      }
      if (should_close_out) {
        close(out_con);
      }
      cpp11::stop("Iconv initialisation failed");
    }
  }

  size_t avail = VROOM_BUFSIZ * 4;

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

      /* Now write out the byte sequence to get into the
         initial state if this is necessary.  */
      Riconv(cd, nullptr, nullptr, &wrptr, &avail);

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
        if (should_close_in) {
          close(in_con);
        }
        if (should_close_out) {
          close(out_con);
        }
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
    if (should_close_in) {
      close(in_con);
    }
    if (should_close_out) {
      close(out_con);
    }
    cpp11::stop("Iconv closed failed");
  }

  if (should_close_in) {
    close(in_con);
  }
  if (should_close_out) {
    close(out_con);
  }

  return bytes_wrote;
}
