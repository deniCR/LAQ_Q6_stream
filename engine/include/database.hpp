/*
 * Copyright (c) 2018 João Afonso. All rights reserved. 
 * From https://github.com/Hubble83/dbms
 */
#ifndef ENGINE_INCLUDE_DATABASE_H_
#define ENGINE_INCLUDE_DATABASE_H_

#include <fstream>
#include <map>
#include <string>
#include <vector>
#include "matrix.hpp"
#include "types.hpp"

namespace engine {

struct Database {
  std::string data_path;
  std::string database_name;
  std::map<std::string, std::map<std::string, std::vector<std::string>>> tables;

  Database(std::string dataPath, std::string databaseName, bool save);

  bool load();
  bool save();

  bool isMeasure(const std::string& table, const std::string attribute);
  bool isDimension(const std::string& table, const std::string attribute);

  void
  createTable(const std::string& tableName,
              const std::map<std::string,std::vector<std::string>>& attributes);

  // attributes: map of attribute name -> column in the file
  void copyFrom(const std::string& inFilePath,
                const std::string& outTable,
                const std::map<Size, std::string>& attributes,
                const char delimiter);
};

}  // namespace engine

#endif  // ENGINE_INCLUDE_DATABASE_H_
