/*
 * Copyright (c) 2018 João Afonso. All rights reserved. 
 * From https://github.com/Hubble83/dbms
 * Changed by Daniel Rodrigues.
 */
#ifndef ENGINE_INCLUDE_KRAO_H_
#define ENGINE_INCLUDE_KRAO_H_

#include <vector>
#include "block.hpp"

namespace engine {

void krao(const FilteredBitVectorBlock& A,
          const DecimalVectorBlock& B,
          FilteredDecimalVectorBlock *C);

void krao(const FilteredBitVectorBlock& A,
          const FilteredBitVectorBlock& B,
          FilteredBitVectorBlock *C);

void krao(const FilteredDecimalVectorBlock& A,
          const FilteredBitVectorBlock& B,
          FilteredDecimalVectorBlock* C);

void krao(const FilteredBitVectorBlock& A,
          const BitmapBlock& B,
          FilteredBitmapBlock *C);

void krao(const FilteredBitmapBlock& A,
          const BitmapBlock& B,
          FilteredBitmapBlock *C,
          Size Bnrows);

void krao(const BitmapBlock& A,
          const FilteredBitmapBlock& B,
          FilteredBitmapBlock *C,
          Size Bnrows);

void krao(const FilteredBitmapBlock& A,
          const FilteredBitVectorBlock& B,
          FilteredBitmapBlock* C);

void krao(const FilteredBitmapBlock& A,
          const DecimalVectorBlock& B,
          FilteredDecimalMapBlock *C);

}  // namespace engine

#endif  // ENGINE_INCLUDE_KRAO_H_
