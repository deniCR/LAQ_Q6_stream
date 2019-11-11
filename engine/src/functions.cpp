/*
 * Copyright (c) 2018 João Afonso. All rights reserved. 
 * From https://github.com/Hubble83/dbms
 */
#include "include/functions.hpp"
#include <regex>
#include <string>

bool match(engine::Literal str, engine::Literal pattern) {
  if(str.size() > 0 && pattern.size() > 0) {
    return std::regex_match(str, std::regex(pattern));
  }
  return false;
}
