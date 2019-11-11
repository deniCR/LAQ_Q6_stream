/*
 * Copyright (c) 2018 João Afonso. All rights reserved. 
 * From https://github.com/Hubble83/dbms
 * Changed by Daniel Rodrigues.
 */
#include "include/krao.hpp"

namespace engine {

void krao(const FilteredBitVectorBlock& A,
          const DecimalVectorBlock& B,
          FilteredDecimalVectorBlock *C) {
  Size i, nnz = 0;
  Size a_cols_size = A.cols.size(), a_nnz = A.nnz;

  for (i = 0; i < a_cols_size - 1 && A.cols[i] < a_nnz; ++i) {
    if ( A.cols[i] < A.cols[i+1] ) {
      C->values[nnz] = B.values[i];
      ++nnz;
    }
  }

  C->cols = A.cols;
  C->nnz = nnz;
}

// Otimização NNZ ...
void krao(const FilteredBitVectorBlock& A,
          const FilteredBitVectorBlock& B,
          FilteredBitVectorBlock *C) {
  Size i, nnz = 0;
  Size a_cols_size = A.cols.size(), a_nnz = A.nnz;

  if (A.nnz < B.nnz) {
    for (i = 0; i < a_cols_size - 1 && A.cols[i] < a_nnz; ++i) {
      C->cols[i] = nnz;
      if ( (A.cols[i] < A.cols[i+1]) && (B.cols[i] < B.cols[i+1]) ) {
        ++nnz;
      }
    }
  } else {
    for (i = 0; i < a_cols_size - 1 && A.cols[i] < a_nnz; ++i) {
      C->cols[i] = nnz;
      if ( (B.cols[i] < B.cols[i+1]) && (A.cols[i] < A.cols[i+1]) ) {
        ++nnz;
      }
    }
  }

  C->cols[i] = nnz;
  C->nnz = nnz;
}

//Select ...
void krao(const FilteredDecimalVectorBlock& A,
          const FilteredBitVectorBlock& B,
          FilteredDecimalVectorBlock* C) {
  Size i, nnz = 0;
  Size a_cols_size = A.cols.size(), a_nnz = A.nnz;

  if (A.nnz < B.nnz) {
    for (i = 0; i < a_cols_size - 1 && A.cols[i] < a_nnz; ++i) {
      C->cols[i] = nnz;
      if ( (A.cols[i] < A.cols[i+1]) && (B.cols[i] < B.cols[i+1]) ) {
        C->values[nnz] = A.values[A.cols[i]];
        ++nnz;
      }
    }
  } else {
    for (i = 0; i < a_cols_size - 1 && A.cols[i] < a_nnz; ++i) {
      C->cols[i] = nnz;
      if ( (B.cols[i] < B.cols[i+1]) && (A.cols[i] < A.cols[i+1]) ) {
        C->values[nnz] = A.values[A.cols[i]];
        ++nnz;
      }
    }
  }

  C->cols[i] = nnz;
  C->nnz = nnz;
}

//Select rows
void krao(const FilteredBitVectorBlock& A,
          const BitmapBlock& B,
          FilteredBitmapBlock *C) {
  Size i, nnz = 0;
  Size a_cols_size = A.cols.size(), a_nnz = A.nnz;

  for (i = 0; i < a_cols_size - 1 && A.cols[i] < a_nnz; ++i) {
    if ( A.cols[i] < A.cols[i+1] ) {
      C->rows[nnz] = B.rows[i];
      ++nnz;
    }
  }

  C->cols = A.cols;
  C-> nnz = nnz;
}

//Merge rows
void krao(const FilteredBitmapBlock& A,
          const BitmapBlock& B,
          FilteredBitmapBlock *C,
          Size Bnrows) {
  Size i, nnz = 0;
  Size a_cols_size = A.cols.size(), a_nnz = A.nnz;

  for (i = 0; i < a_cols_size - 1 && A.cols[i] < a_nnz; ++i) {
    if ( A.cols[i] < A.cols[i+1] ) {
      C->rows[nnz] = (A.rows[A.cols[i]] * Bnrows) + B.rows[i];
      ++nnz;
    }
  }

  C->cols = A.cols;
  C->nnz = nnz;
}

//Select ...
void krao(const BitmapBlock& A,
          const FilteredBitmapBlock& B,
          FilteredBitmapBlock *C,
          Size Bnrows) {
  Size i, nnz = 0;
  Size b_cols_size = B.cols.size(), b_nnz = B.nnz;
  
  for (i = 0; i < b_cols_size - 1 && B.cols[i] < b_nnz; ++i) {
    if ( B.cols[i] < B.cols[i+1] ) {
      C->rows[nnz] = (A.rows[i] * Bnrows) + B.rows[B.cols[i]];
      ++nnz;
    }
  }

  C->cols = B.cols;
  C-> nnz = nnz;
}

//Select ...
void krao(const FilteredBitmapBlock& A,
          const FilteredBitVectorBlock& B,
          FilteredBitmapBlock* C) {
  Size i, nnz = 0;
  Size a_cols_size = A.cols.size(), a_nnz = A.nnz;

  if (A.nnz < B.nnz) {
    for (i = 0; i < a_cols_size - 1 && A.cols[i] < a_nnz; ++i) {
      C->cols[i] = nnz;
      if ( (A.cols[i] < A.cols[i+1]) && (B.cols[i] < B.cols[i+1]) ) {
        C->rows[nnz] = A.rows[A.cols[i]];
        ++nnz;
      }
    }
  } else {
    for (i = 0; i < a_cols_size - 1 && A.cols[i] < a_nnz; ++i) {
      C->cols[i] = nnz;
      if ( (B.cols[i] < B.cols[i+1]) && (A.cols[i] < A.cols[i+1]) ) {
        C->rows[nnz] = A.rows[A.cols[i]];
        ++nnz;
      }
    }
  }

  C->cols[i] = nnz;
  C->nnz = nnz;
}

//Merge rows and values
void krao(const FilteredBitmapBlock& A,
          const DecimalVectorBlock& B,
          FilteredDecimalMapBlock *C) {
  Size i, nnz = 0;
  Size a_cols_size = A.cols.size(), a_nnz = A.nnz;

  for (i = 0; i < a_cols_size - 1 && A.cols[i] < a_nnz; ++i) {
    if ( A.cols[i] < A.cols[i+1] ) {
      C->values[nnz] = B.values[i];
      ++nnz;
    }
  }

  C->cols = A.cols;
  C->rows = A.rows;
  C->nnz = nnz;
}

}  // namespace engine
