/*
 * Copyright (c) 2018 João Afonso. All rights reserved. 
 * From https://github.com/Hubble83/dbms
 */
#ifndef ENGINE_INCLUDE_DOT_H_
#define ENGINE_INCLUDE_DOT_H_

#include <vector>
#include "block.hpp"
#include "matrix.hpp"

namespace engine {

void dot(const FilteredBitVector& A,
         const BitmapBlock& B,
         FilteredBitVectorBlock *C);

void dot(const FilteredBitmap& A,
         const BitmapBlock& B,
         FilteredBitmapBlock *C);

}  // namespace engine

#endif  // ENGINE_INCLUDE_DOT_H_
