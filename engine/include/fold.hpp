/*
 * Copyright (c) 2018 João Afonso. All rights reserved. 
 * From https://github.com/Hubble83/dbms
 * Changed by Daniel Rodrigues.
 */
#ifndef ENGINE_INCLUDE_FOLD_H_
#define ENGINE_INCLUDE_FOLD_H_

#include "block.hpp"
#include "matrix.hpp"

namespace engine {

Decimal sum(const FilteredDecimalVectorBlock& in);

void sum(const FilteredDecimalVectorBlock& in,
         Decimal *acc);

void sum(const FilteredDecimalMapBlock& in,
         FilteredDecimalMapAcc *acc);

}  // namespace engine

#endif  // ENGINE_INCLUDE_FOLD_H_
