/*
 * Copyright (c) 2018 João Afonso. All rights reserved. 
 * From https://github.com/Hubble83/dbms
 * Changed by Daniel Rodrigues.
 */
#include "include/lift.hpp"
#include <vector>

namespace engine {

void lift(Decimal(*f)(std::vector<Decimal> *),
          std::vector<DecimalVectorBlock*> *in,
          DecimalVectorBlock* out) {
  std::vector<Decimal> v(in->size());
  Size in_nnz = (*in)[0]->nnz;
  Size in_size = in->size();

  for (Size i = 0; i < in_nnz; ++i) {

    v[0] = (*in)[0]->values[i];
    v[1] = (*in)[1]->values[i];

    out->values[i] = (*f)(&v);
  }
  out->nnz = in_nnz;
}

void lift(Decimal(*f)(std::vector<Decimal> *),
          std::vector<DecimalVectorBlock*> *in,
          const FilteredBitVectorBlock& iter,
          FilteredDecimalVectorBlock* out) {
  std::vector<Decimal> v(in->size());
  Size iter_cols = iter.cols.size();
  Size in_size = in->size();

  Size i, nnz = 0;
  for (i = 0; i < iter_cols; ++i) {
    out->cols[i] = nnz;
    if (iter.cols[i+1] > iter.cols[i]) {
      for (Size j = 0; j < in_size; ++j) {
        v[j] = (*in)[j]->values[i];
      }
      out->values[nnz++] = (*f)(&v);
    }
  }
  out->cols[i] = nnz;
  out->nnz = nnz;
}

}  // namespace engine
