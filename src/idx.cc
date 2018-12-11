#include <Rcpp.h>
#include <fcntl.h>
#include <fstream>
#include <stdio.h>
#include <sys/stat.h>
#include <unistd.h>

#if R_VERSION < R_Version(3, 6, 0)

// workaround because R's <R_ext/Altrep.h> not so conveniently uses `class`
// as a variable name, and C++ is not happy about that
//
// SEXP R_new_altrep(R_altrep_class_t class, SEXP data1, SEXP data2);
//
#define class klass

// Because functions declared in <R_ext/Altrep.h> have C linkage
extern "C" {
#include <R_ext/Altrep.h>
}

// undo the workaround
#undef class

#else
#include <R_ext/Altrep.h>
#endif

using namespace Rcpp;

// inspired by Luke Tierney and the R Core Team
// https://github.com/ALTREP-examples/Rpkg-mutable/blob/master/src/mutable.c
// and Romain François
// https://purrple.cat/blog/2018/10/21/lazy-abs-altrep-cplusplus/ and Dirk

struct readidx_string {

  static R_altrep_class_t class_t;

  // Make an altrep object of class `stdvec_double::class_t`
  static SEXP Make(std::vector<size_t>* offsets, const char* filename) {

    // `out` and `xp` needs protection because R_new_altrep allocates
    SEXP out = PROTECT(Rf_allocVector(VECSXP, 2));

    SEXP xp = PROTECT(R_MakeExternalPtr(offsets, R_NilValue, R_NilValue));
    R_RegisterCFinalizerEx(xp, readidx_string::Finalize, TRUE);

    SET_VECTOR_ELT(out, 0, xp);
    SET_VECTOR_ELT(out, 1, Rf_mkString(filename));

    // make a new altrep object of class `readidx_string::class_t`
    SEXP res = R_new_altrep(class_t, out, R_NilValue);

    UNPROTECT(2);

    return res;
  }

  // finalizer for the external pointer
  static void Finalize(SEXP xp) {
    delete static_cast<std::vector<size_t>*>(R_ExternalPtrAddr(xp));
  }

  static std::vector<size_t>* Ptr(SEXP x) {
    return static_cast<std::vector<size_t>*>(
        R_ExternalPtrAddr(VECTOR_ELT(R_altrep_data1(x), 0)));
  }

  // same, but as a reference, for convenience
  static std::vector<size_t>& Get(SEXP vec) { return *Ptr(vec); }

  static const char* Filename(SEXP vec) {
    return CHAR(STRING_ELT(VECTOR_ELT(R_altrep_data1(vec), 1), 0));
  }

  // ALTREP methods -------------------

  // The length of the object
  static R_xlen_t Length(SEXP vec) { return Get(vec).size(); }

  // What gets printed when .Internal(inspect()) is used
  static Rboolean Inspect(
      SEXP x,
      int pre,
      int deep,
      int pvec,
      void (*inspect_subtree)(SEXP, int, int, int)) {
    Rprintf(
        "readidx_string (file=%s, len=%d, ptr=%p)\n",
        Filename(x),
        Length(x),
        Ptr(x));
    return TRUE;
  }

  // ALTSTRING methods -----------------

  // the element at the index `i`
  //
  // this does not do bounds checking because that's expensive, so
  // the caller must take care of that
  static SEXP string_Elt(SEXP vec, R_xlen_t i) {
    const char* filename = Filename(vec);
    const std::vector<size_t>& idx = Get(vec);

    size_t cur_loc = idx[i];
    size_t next_loc = idx[i + 1];
    size_t len = next_loc - cur_loc;
    // Rcerr << cur_loc << ':' << next_loc << ':' << len << '\n';

    int fd = open(filename, O_RDONLY);
    lseek(fd, cur_loc, SEEK_SET);

    char buffer[2048];
    read(fd, buffer, len - 1);
    close(fd);

    return Rf_mkCharLenCE(buffer, len - 1, CE_UTF8);
  }

  // -------- initialize the altrep class with the methods above

  static void Init(DllInfo* dll) {
    class_t = R_make_altstring_class("readidx_string", "reagidx", dll);

    // altrep
    R_set_altrep_Length_method(class_t, Length);
    R_set_altrep_Inspect_method(class_t, Inspect);

    // altvec
    // R_set_altvec_Dataptr_method(class_t, Dataptr);
    // R_set_altvec_Dataptr_or_null_method(class_t, Dataptr_or_null);

    // altstring
    R_set_altstring_Elt_method(class_t, string_Elt);
  }
};

R_altrep_class_t readidx_string::class_t;

// Called the package is loaded (needs Rcpp 0.12.18.3)
// [[Rcpp::init]]
void init_readidx_string(DllInfo* dll) { readidx_string::Init(dll); }

SEXP resize(SEXP in, size_t n) {
  Rcerr << "Resizing to: " << n << std::endl;
  size_t sz = Rf_xlength(in);
  if (sz == n)
    return in;

  if (n > 0 && n < sz) {
    SETLENGTH(in, n);
    SET_TRUELENGTH(in, n);
  } else {
    in = Rf_xlengthgets(in, n);
  }
  return in;
}

/**
 * Get the size of a file.
 * @param filename The name of the file to check size for
 * @return The filesize, or 0 if the file does not exist.
 */
size_t get_file_size(const std::string& filename) {
  struct stat st;
  if (stat(filename.c_str(), &st) != 0) {
    return 0;
  }
  return st.st_size;
}

size_t guess_size(size_t records, size_t bytes, size_t file_size) {
  double percent_complete = (double)(bytes) / file_size;
  size_t total_records = records / percent_complete * 1.1;
  return total_records;
}

// [[Rcpp::export]]
SEXP create_index(std::string filename) {
  // TODO: probably change this to something like 1024
  std::vector<size_t>* out = new std::vector<size_t>();
  out->reserve(96);

  size_t columns = 0;
  size_t file_size = get_file_size(filename);
  size_t file_offset = 0;

  size_t prev_loc = 0;

  // First try opening the index
  std::string idx_file = filename + ".idx";

  // From https://stackoverflow.com/a/17925143/2055486
  const size_t BUFFER_SIZE = 16 * 1024;
  int fd = open(filename.c_str(), O_RDONLY);

  /* Advise the kernel of our access pattern.  */

  char buf[BUFFER_SIZE + 1];

  // TODO: consider safe_read
  // (https://github.com/coreutils/gnulib/blob/master/lib/safe-read.c)
  while (size_t bytes_read = read(fd, buf, BUFFER_SIZE)) {
    if (!bytes_read)
      break;

    char* p = buf;
    char* end = p + bytes_read;
    while (p != end) {
      switch (*p) {
      case '\n': {
        if (columns == 0) {
          columns = out->size() + 1;
          size_t cur_loc = file_offset + (p - buf + 1);

          size_t new_size = guess_size(out->size(), cur_loc, file_size);
          out->reserve(new_size);
        }
      }
      case '\t': {
        size_t cur_loc = file_offset + (p - buf + 1);
        out->push_back(prev_loc);
        prev_loc = cur_loc;
        break;
      }
      }
      ++p;
    }
    file_offset += bytes_read;
  }
  out->push_back(file_size);

  close(fd);

  int fd_out = open(idx_file.c_str(), O_WRONLY | O_CREAT, 0644);
  write(fd_out, out->data(), out->size());
  close(fd_out);

  return readidx_string::Make(out, filename.c_str());
}
