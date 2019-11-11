/*
 * Copyright (c) 2018 João Afonso. All rights reserved. 
 * From https://github.com/Hubble83/dbms
 * Changed by Daniel Rodrigues.
 */
#ifndef ENGINE_INCLUDE_FILTER_H_
#define ENGINE_INCLUDE_FILTER_H_

#include <vector>
#include "block.hpp"
#include "types.hpp"

namespace engine {

void filter(bool(*f)(std::vector<Decimal> *),
            DecimalVectorBlock* in,
            FilteredBitVectorBlock* out);

void filter(bool(*f)(std::vector<Decimal> *),
            std::vector<DecimalVectorBlock*> *in,
            FilteredBitVectorBlock* out);

void filter(bool(*f)(std::vector<Literal> *),
            std::vector<LabelBlock*> *in,
            FilteredBitVectorBlock* out);

void filter(bool(*f)(std::vector<Literal> *),
            LabelBlock *in,
            FilteredBitVectorBlock* out);

void filter(bool(*f)(std::vector<Decimal> *),
            std::vector<DecimalVectorBlock*> *in,
            const FilteredBitVectorBlock& iter,
            FilteredBitVectorBlock* out);
}
#endif  // ENGINE_INCLUDE_LIFT_H_
