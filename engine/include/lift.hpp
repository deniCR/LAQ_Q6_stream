/*
 * Copyright (c) 2018 João Afonso. All rights reserved. 
 * From https://github.com/Hubble83/dbms
 * Changed by Daniel Rodrigues.
 */
#ifndef ENGINE_INCLUDE_LIFT_H_
#define ENGINE_INCLUDE_LIFT_H_

#include <vector>
#include "block.hpp"

namespace engine {

void lift(Decimal(*f)(std::vector<Decimal> *),
          std::vector<DecimalVectorBlock*> *in,
          DecimalVectorBlock* out);

void lift(Decimal(*f)(std::vector<Decimal>),
          std::vector<DecimalVectorBlock*> *in,
          DecimalVectorBlock* out);

void lift(Decimal(*f)(std::vector<Decimal>),
          const std::vector<DecimalVectorBlock>& in,
          DecimalVectorBlock* out);

void lift(Decimal(*f)(std::vector<Decimal> *),
          const std::vector<DecimalVectorBlock>& in,
          DecimalVectorBlock* out);

void lift(Decimal(*f)(std::vector<Decimal> *),
          const std::vector<DecimalVectorBlock>& in,
          const FilteredBitVectorBlock& iter,
          FilteredDecimalVectorBlock* out);

}  // namespace engine

#endif  // ENGINE_INCLUDE_LIFT_H_