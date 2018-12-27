#pragma once

class vroom_vec {

public:
  // finalizer for the external pointer
  static void Finalize_Idx(SEXP xp) {
    auto vec_p = static_cast<std::shared_ptr<std::vector<size_t> >*>(
        R_ExternalPtrAddr(xp));
    // Rcpp::Rcerr << "real_idx_ptr:" << vec_p->use_count() << '\n';
    delete vec_p;
  }

  static void Finalize_Mmap(SEXP mmap) {
    auto mmap_p =
        static_cast<mio::shared_mmap_source*>(R_ExternalPtrAddr(mmap));
    // Rcpp::Rcerr << "mmap_ptr:" << mmap_p.use_count() << '\n';
    delete mmap_p;
  }

  static mio::shared_mmap_source* Mmap(SEXP x) {
    return static_cast<mio::shared_mmap_source*>(
        R_ExternalPtrAddr(VECTOR_ELT(R_altrep_data1(x), 1)));
  }

  static std::shared_ptr<std::vector<size_t> > Idx(SEXP x) {
    return *static_cast<std::shared_ptr<std::vector<size_t> >*>(
        R_ExternalPtrAddr(VECTOR_ELT(R_altrep_data1(x), 0)));
  }

  static const R_xlen_t Column(SEXP vec) {
    return REAL(VECTOR_ELT(R_altrep_data1(vec), 2))[0];
  }

  static const R_xlen_t Num_Columns(SEXP vec) {
    return REAL(VECTOR_ELT(R_altrep_data1(vec), 3))[0];
  }

  static const R_xlen_t Skip(SEXP vec) {
    return REAL(VECTOR_ELT(R_altrep_data1(vec), 4))[0];
  }

  static const R_xlen_t Num_Threads(SEXP vec) {
    return REAL(VECTOR_ELT(R_altrep_data1(vec), 5))[0];
  }

  // ALTREP methods -------------------

  // The length of the object
  static R_xlen_t Length(SEXP vec) {
    return (Idx(vec)->size() / Num_Columns(vec)) - Skip(vec);
  }

  // ALTVec methods -------------------

  static const void* Dataptr_or_null(SEXP vec) {
    SEXP data2 = R_altrep_data2(vec);
    if (data2 == R_NilValue)
      return nullptr;

    return STDVEC_DATAPTR(data2);
  }
};
