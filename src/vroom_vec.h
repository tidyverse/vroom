#pragma once

struct vroom_vec_info {
  std::shared_ptr<std::vector<size_t> > idx;
  mio::shared_mmap_source mmap;
  R_xlen_t column;
  R_xlen_t num_columns;
  R_xlen_t skip;
  R_xlen_t num_threads;
};

class vroom_vec {

public:
  // finalizer for the external pointer
  static void Finalize(SEXP xp) {
    auto info_p = static_cast<vroom_vec_info*>(R_ExternalPtrAddr(xp));
    // Rcpp::Rcerr << "real_idx_ptr:" << vec_p->use_count() << '\n';
    delete info_p;
  }

  static vroom_vec_info& Info(SEXP x) {
    return *static_cast<vroom_vec_info*>(R_ExternalPtrAddr(R_altrep_data1(x)));
  }

  // ALTREP methods -------------------

  // The length of the object
  static R_xlen_t Length(SEXP vec) {
    auto inf = Info(vec);
    return (inf.idx->size() / inf.num_columns) - inf.skip;
  }

  // ALTVec methods -------------------

  static const void* Dataptr_or_null(SEXP vec) {
    SEXP data2 = R_altrep_data2(vec);
    if (data2 == R_NilValue)
      return nullptr;

    return STDVEC_DATAPTR(data2);
  }
};
