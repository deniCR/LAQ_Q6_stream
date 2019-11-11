/*
 * Copyright (c) 2018 João Afonso. All rights reserved. 
 * From https://github.com/Hubble83/dbms
 * Changed by Daniel Rodrigues.
 */
#include <map>
#include "include/fold.hpp"

namespace engine {

Decimal sum(const FilteredDecimalVectorBlock& in) {
  Decimal acc=0;
  Size size = in.nnz;
  for (int i = 0; i < size; ++i)
  {
    acc += in.values[i];
  }
  return acc;
}

void sum(const FilteredDecimalVectorBlock& in,
         Decimal *acc) {
  Size size = in.nnz;
  for (int i = 0; i < size; ++i)
  {
    (*acc) += in.values[i];
  }
}

void sum(const FilteredDecimalMapBlock& in,
         FilteredDecimalMapAcc *acc) {

  Size size = in.values.size();
  //std::map<MultiPrecision,Decimal> map;
  for (Size i=0; i < size; ++i) {
    // operator [] searches for an element
    // and initializes it if it does not exist
    acc->map[in.rows[i]] += in.values[i];
  }

  /*
  void getMatrix(FilteredDecimalMapAcc& acc, FilteredDecimalMap *mat) {
    Size nnz = 0;
    for (const auto& kv : acc.map) {
      mat->rows[nnz] = kv.first;
      mat->values[nnz] = kv.second;
      ++nnz;
    }
    mat->nnz = nnz;
    mat->cols = {0, nnz};
  }
  */

}

}  // namespace engine
