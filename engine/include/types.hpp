/*
 * Copyright (c) 2018 João Afonso. All rights reserved. 
 * From https://github.com/Hubble83/dbms
 */
#ifndef ENGINE_INCLUDE_TYPES_H_
#define ENGINE_INCLUDE_TYPES_H_

#include <string>

namespace engine {

// TODO(João): Use a cross platform lib to have coherent basic types
// Use GMP or other to represent multi precision

// Decimal default constructor must set it to zero
typedef double Decimal;
typedef uint64_t MultiPrecision;
typedef std::string Literal;
typedef size_t Size;

#ifndef BSIZE
#define BSIZE 32768
#endif

}  // namespace engine

#endif  // ENGINE_INCLUDE_TYPES_H_
