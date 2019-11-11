#ifndef ENGINE_INCLUDE_FOLD_OLD_H_
#define ENGINE_INCLUDE_FOLD_OLD_H_

#include "block.hpp"
#include "matrix.hpp"

namespace engine {

void sum_old(const FilteredDecimalVectorBlock& in,
         Decimal *acc);

void sum_old(const FilteredDecimalMapBlock& in,
         FilteredDecimalMapAcc *acc);

}

#endif