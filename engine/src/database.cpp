/*
 * Copyright (c) 2018 João Afonso. All rights reserved. 
 * From https://github.com/Hubble83/dbms
 */
#include "include/database.hpp"
#include <sys/stat.h>
#include <sys/types.h>
#include <cereal/archives/binary.hpp>
#include <cereal/types/map.hpp>
#include <cereal/types/string.hpp>
#include <cereal/types/vector.hpp>
#include <experimental/filesystem>
#include <map>
#include <string>
#include <vector>
#include "include/matrix.hpp"
#include "include/types.hpp"

namespace engine {

Database::Database(std::string dataPath, std::string databaseName, bool save) {
  data_path = dataPath;
  database_name = databaseName;

  if (save) {
    mode_t mode = 0733;
    int error = 0;
    #if defined(_WIN32)
      error = _mkdir((dataPath + "/" + databaseName).c_str());
    #else
      error = mkdir((dataPath + "/" + databaseName).c_str(), mode);
    #endif
    if (error != 0) {
      // handle error here
    }

    this->save();
  }
}

bool Database::load() {
  std::ifstream ifs(data_path + "/" + database_name + "/meta.dat");
  if (ifs.is_open()) {
    {
      cereal::BinaryInputArchive iarchive(ifs);
      iarchive(data_path, database_name, tables);
    }
    ifs.close();
    return true;
  }
  return false;
}

bool Database::save() {
  std::ofstream ofs(data_path + "/" + database_name + "/meta.dat");
  if (ofs.is_open()) {
    {
      cereal::BinaryOutputArchive oarchive(ofs);
      oarchive(data_path, database_name, tables);
    }
    ofs.close();
    return true;
  }
  return false;
}

bool Database::isMeasure(const std::string& table,
                         const std::string attribute) {
  return tables[table][attribute][0] == "measure";
}

bool Database::isDimension(const std::string& table,
                           const std::string attribute) {
  return tables[table][attribute][0] == "dimension";
}

void Database::createTable(
  const std::string& tableName,
  const std::map<std::string,std::vector<std::string>>& attributes) {
  tables[tableName] = attributes;

  std::string path = data_path + "/" + database_name + "/" + tableName;
  mode_t mode = 0733;
  int error = 0;
  #ifdef _WIN32
    error = _mkdir(path.c_str());
  #else
    error = mkdir(path.c_str(), mode);
  #endif

  for (const auto& attr : attributes) {
    #ifdef _WIN32
      error = _mkdir((path + "/" + attr.first).c_str());
      error = _mkdir((path + "/" + attr.first + "/blocks").c_str());
    #else
      error = mkdir((path + "/" + attr.first).c_str(), mode);
      error = mkdir((path + "/" + attr.first + "/blocks").c_str(), mode);
    #endif
    if (attr.second[0] == "measure") {
      DecimalVector v(data_path, database_name, tableName, attr.first);
      v.save();
    } else if (attr.second[0] == "dimension") {
      #ifdef _WIN32
        error = _mkdir((path + "/" + attr.first + "/labels").c_str());
      #else
        error = mkdir((path + "/" + attr.first + "/labels").c_str(), mode);
      #endif
      Bitmap b(data_path, database_name, tableName, attr.first);
      b.save();
    }
  }
  if (error != 0) {
    // handle error here
  }
  this->save();
}

void Database::copyFrom(const std::string& inFilePath,
                        const std::string& outTable,
                        const std::map<Size, std::string>& attributes,
                        const char delimiter) {
  std::ifstream input(inFilePath);

  if (input.is_open()) {
    std::string line;

    std::map<std::string, Matrix*> matrices;

    for (const auto& attr : attributes) {
      if (tables.at(outTable).at(attr.second).at(0) == "measure") {
        matrices[attr.second] = new DecimalVector(data_path,
                                                  database_name,
                                                  outTable,
                                                  attr.second);
      } else if (tables.at(outTable).at(attr.second).at(0) == "dimension") {
        matrices[attr.second] = new Bitmap(data_path,
                                           database_name,
                                           outTable,
                                           attr.second);
      }
    }


    while (std::getline(input, line)) {
      std::string value;
      std::stringstream ss(line);

      for (Size i=0; std::getline(ss, value, delimiter); ++i) {
        if (attributes.find(i) != attributes.end()) {
          if (tables.at(outTable).at(attributes.at(i)).at(0) == "measure") {
            Decimal dec = std::stod(value);
            DecimalVector *v =
              reinterpret_cast<DecimalVector*>(matrices[attributes.at(i)]);
            v->insert((Decimal) dec);

          } else if (
              tables.at(outTable).at(attributes.at(i)).at(0) == "dimension") {
            Bitmap *b =
              reinterpret_cast<Bitmap*>(matrices[attributes.at(i)]);
            b->insert((Literal) value);
          }
        }
      }
    }

    for (const auto& mat : matrices) {
      if (tables.at(outTable).at(mat.first).at(0) == "measure") {
        DecimalVector *v =
              reinterpret_cast<DecimalVector*>(mat.second);
        v->saveLastBlock();
      } else if (tables.at(outTable).at(mat.first).at(0) == "dimension") {
        Bitmap *b =
              reinterpret_cast<Bitmap*>(mat.second);
        b->saveLastBlock();
        b->saveLastLabelBlock();
        b->saveLabelHash();
      }
      mat.second->save();
    }
  }
}

}  // namespace engine
