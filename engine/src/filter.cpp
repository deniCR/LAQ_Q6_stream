/*
 * Copyright (c) 2018 João Afonso. All rights reserved. 
 * From https://github.com/Hubble83/dbms
 * Changed by Daniel Rodrigues.
 */
#include <vector>
#include "include/filter.hpp"
#include "include/types.hpp"

namespace engine {

void filter(bool(*f)(std::vector<Decimal> *),
            DecimalVectorBlock* in,
            FilteredBitVectorBlock* out) {
  std::vector<Decimal> v(1);
  Size i, nnz = 0;
  Size in_nnz = in->nnz, in_size = 1;
  
  for (i = 0; i < in_nnz; ++i) {
    out->cols[i] = nnz;
    (v)[0] = in->values[i];
    if ((*f)(&v)) {
      ++nnz;
    }
  }
  out->cols[i] = nnz;
  out->nnz = nnz;
}

void filter(bool(*f)(std::vector<Decimal> *),
            std::vector<DecimalVectorBlock*> *in,
            FilteredBitVectorBlock* out) {
  std::vector<Decimal> v(in->size());
  Size i, nnz = 0;
  Size in_nnz = (*in)[0]->nnz, in_size = in->size();
  
  for (i = 0; i < in_nnz; ++i) {
    out->cols[i] = nnz;
    for (Size j = 0; j < in_size; ++j) {
      (v)[j] = (*in)[j]->values[i];
    }
    if ((*f)(&v)) {
      ++nnz;
    }
  }
  out->cols[i] = nnz;
  out->nnz = nnz;
}

void filter(bool(*f)(std::vector<Literal> *),
            std::vector<LabelBlock*> *in,
            FilteredBitVectorBlock* out) {
  std::vector<Literal> v(in->size());
  Size i, nnz = 0;
  Size in_nnz = (*in)[0]->nnz, in_size = in->size();

  for (i = 0; i < in_nnz; ++i) {
    out->cols[i] = nnz;
    for (Size j = 0; j < in_size; ++j) {
      v[j] = (*in)[j]->labels[i];
    }
    if ((*f)(&v)) {
      ++nnz;
    }
  }
  out->cols[i] = nnz;
  out->nnz = nnz;
}

void filter(bool(*f)(std::vector<Literal> *),
            LabelBlock *in,
            FilteredBitVectorBlock* out) {
  std::vector<Literal> v(1);
  Size i, nnz = 0;
  Size in_nnz = in->nnz, in_size = 1;

  for (i = 0; i < in_nnz; ++i) {
    out->cols[i] = nnz;
      v[0] = in->labels[i];
    if ((*f)(&v)) {
      ++nnz;
    }
  }
  out->cols[i] = nnz;
  out->nnz = nnz;
}

void filter(bool(*f)(std::vector<Decimal> *),
            std::vector<DecimalVectorBlock*> *in,
            const FilteredBitVectorBlock& iter,
            FilteredBitVectorBlock* out) {
  std::vector<Decimal> v(in->size());
  Size i, nnz = 0;
  Size iter_cols = iter.cols.size(), in_size = in->size();

  for (i = 0; i < iter_cols; ++i) {
    out->cols[i] = nnz;
    if (iter.cols[i+1] > iter.cols[i]) {
      for (Size j = 0; j < in_size; ++j) {
        v[j] = (*in)[j]->values[i];
      }
      if ((*f)(&v)) {
        ++nnz;
      }
    }
  }
  out->cols[i] = nnz;
  out->nnz = nnz;
}

}  // namespace engine
