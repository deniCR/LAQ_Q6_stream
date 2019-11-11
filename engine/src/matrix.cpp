/*
 * Copyright (c) 2018 João Afonso. All rights reserved. 
 * From https://github.com/Hubble83/dbms
 * Changed by Daniel Rodrigues.
 */
#include <cereal/archives/binary.hpp>
#include <cereal/types/map.hpp>
#include <cereal/types/string.hpp>
#include <string>
#include "include/block.hpp"
#include "include/matrix.hpp"
#include "include/types.hpp"

namespace engine {

Matrix::Matrix(Size n_blocks) {
  nnz = 0;
  nrows = 0;
  nBlocks = n_blocks;
  nLabelBlocks = 0;
  labelsTable = "";
  labelsAttribute = "";

  dataPath = "";
  database = "";
  table = "";
  attribute = "";
}

Matrix::Matrix(std::string data_path,
               std::string database_name,
               std::string table_name,
               std::string attribute_name) {
  nnz = 0;
  nrows = 0;
  nBlocks = 0;
  nLabelBlocks = 0;
  dataPath = data_path;
  database = database_name;
  table = table_name;
  attribute = attribute_name;

  std::ifstream ifs(getPath() + "/meta.dat", std::ios::in | std::ios::binary);
  if (ifs.is_open()) {
    {
      cereal::BinaryInputArchive iarchive(ifs);
      iarchive(nnz, nrows, nBlocks, nLabelBlocks, labelsTable, labelsAttribute);
    }
    ifs.close();
  }
}

// Add constructor to use in CreateTable

bool Matrix::save() {
  std::ofstream ofs(getPath() + "/meta.dat", std::ios::out | std::ios::binary);
  if (ofs.is_open()) {
    {
      cereal::BinaryOutputArchive oarchive(ofs);
      oarchive(nnz, nrows, nBlocks, nLabelBlocks, labelsTable, labelsAttribute);
    }
    ofs.close();
    return true;
  }
  return false;
}

std::string Matrix::getPath() {
  return dataPath + "/" + database + "/" + table + "/" + attribute;
}

DecimalVector::DecimalVector(Size n_blocks) : Matrix(n_blocks) {
  blocks.reserve(nBlocks);
}

DecimalVector::DecimalVector(std::string data_path,
                             std::string database_name,
                             std::string table_name,
                             std::string attribute_name)
  : Matrix(data_path, database_name, table_name, attribute_name) {
  blocks.reserve(nBlocks);
}

DecimalVector::~DecimalVector() {
  for (auto bl : blocks) {
    delete bl;
  }
}

void DecimalVector::loadBlock(Size idx) {
  std::string path = getPath() + "/blocks/" + std::to_string(idx);
  DecimalVectorBlock* b = new DecimalVectorBlock();
  b->load(path);
  blocks[idx] = b;
}

void DecimalVector::loadBlock(Size idx, DecimalVectorBlock *b) {
  std::string path = getPath() + "/blocks/" + std::to_string(idx);
  b->load(path);
  blocks[idx] = b;
}

void DecimalVector::deleteBlock(Size idx) {
  delete blocks[idx];
}

void DecimalVector::saveBlock(Size idx) {
  std::string path = getPath() + "/blocks/" + std::to_string(idx);
  DecimalVectorBlock* b = reinterpret_cast<DecimalVectorBlock*>(blocks[idx]);
  b->save(path);
}

void DecimalVector::saveLastBlock() {
  if (blocks[nBlocks-1] != nullptr) {
    saveBlock(nBlocks-1);
  }
}

// make inline!
void DecimalVector::insert(Decimal value) {
  if (nnz % BSIZE == 0) {
    if (nBlocks > 0 && blocks[nBlocks-1] != nullptr) {
      this->saveBlock(nBlocks-1);
      this->deleteBlock(nBlocks-1);
    }
    blocks.push_back(new DecimalVectorBlock());
    ++nBlocks;
  } else if (blocks[nBlocks-1] == nullptr) {
    this->loadBlock(nBlocks-1);
  }
  blocks[nBlocks-1]->insert(value);
  ++nnz;
}

void Bitmap::LabelHash::load(std::string attrPath) {
  std::ifstream ifs(attrPath + "/hash.dat", std::ios::in | std::ios::binary);
  if (ifs.is_open()) {
    {
      cereal::BinaryInputArchive iarchive(ifs);
      iarchive(labels, nnz);
    }
    ifs.close();
  }
}

void Bitmap::LabelHash::save(std::string attrPath) {
  std::ofstream ofs(attrPath + "/hash.dat", std::ios::out | std::ios::binary);
  if (ofs.is_open()) {
    {
      cereal::BinaryOutputArchive oarchive(ofs);
      oarchive(labels, nnz);
    }
    ofs.close();
  }
}

// make inline!
bool Bitmap::LabelHash::contains(Literal label) {
  return labels.find(label) != labels.end();
}

// make inline!
MultiPrecision Bitmap::LabelHash::getRow(Literal label) {
  return labels.at(label);
}

// make inline!
void Bitmap::LabelHash::insert(Literal label) {
  if (labels.find(label) == labels.end()) {
    labels[label] = nnz++;
  }
}

Bitmap::Bitmap(Size n_blocks) : Matrix(n_blocks) {
  blocks.reserve(nBlocks);
}

Bitmap::Bitmap(std::string data_path,
               std::string database_name,
               std::string table_name,
               std::string attribute_name)
  : Matrix(data_path, database_name, table_name, attribute_name) {
  blocks.reserve(nBlocks);
  labels.reserve(nLabelBlocks);
  hash.load(getPath());
}

Bitmap::~Bitmap() {
  for (auto bl : blocks) {
    delete bl;
  }
  for (auto lb : labels) {
    delete lb;
  }
}

void Bitmap::loadBlock(Size idx) {
  std::string path = getPath() + "/blocks/" + std::to_string(idx);
  BitmapBlock* b = new BitmapBlock();
  b->load(path);
  blocks[idx] = b;
}

void Bitmap::loadBlock(Size idx, BitmapBlock *b) {
  std::string path = getPath() + "/blocks/" + std::to_string(idx);
  b->load(path);
  blocks[idx] = b;
}

void Bitmap::deleteBlock(Size idx) {
  delete blocks[idx];
}

void Bitmap::saveBlock(Size idx) {
  std::string path = getPath() + "/blocks/" + std::to_string(idx);
  blocks[idx]->save(path);
}

void Bitmap::saveLastBlock() {
  if (blocks[nBlocks-1] != nullptr) {
    saveBlock(nBlocks-1);
  }
}

void Bitmap::loadLabelBlock(Size idx) {
  std::string path = getPath() + "/labels/" + std::to_string(idx);
  LabelBlock* l = new LabelBlock();
  l->load(path);
  labels[idx] = l;
}

void Bitmap::loadLabelBlock(Size idx, LabelBlock *l) {
  std::string path = getPath() + "/labels/" + std::to_string(idx);
  l->load(path);
  labels[idx] = l;
}

void Bitmap::deleteLabelBlock(Size idx) {
  delete labels[idx];
}

void Bitmap::saveLabelBlock(Size idx) {
  std::string path = getPath() + "/labels/" + std::to_string(idx);
  labels[idx]->save(path);
}

void Bitmap::saveLastLabelBlock() {
  if (labels[nLabelBlocks-1] != nullptr) {
    saveLabelBlock(nLabelBlocks-1);
  }
}

void Bitmap::loadLabelHash() {
  hash.load(getPath());
}

void Bitmap::saveLabelHash() {
  hash.save(getPath());
}

void Bitmap::insert(Literal value) {
  if (nnz % BSIZE == 0) {
    if (nBlocks > 0 && blocks[nBlocks-1] != nullptr) {
      this->saveBlock(nBlocks-1);
      this->deleteBlock(nBlocks-1);
    }
    blocks.push_back(new BitmapBlock());
    ++nBlocks;
  }
  if (blocks[nBlocks-1] == nullptr) {
    this->loadBlock(nBlocks-1);
  }
  if (!hash.contains(value)) {
    hash.insert(value);
    if (nrows % BSIZE == 0) {
      if (nLabelBlocks > 0 && labels[nLabelBlocks-1] != nullptr) {
        this->saveLabelBlock(nLabelBlocks-1);
        this->deleteLabelBlock(nLabelBlocks-1);
      }
      labels.push_back(new LabelBlock());
      ++nLabelBlocks;
    } else if (labels[nLabelBlocks-1] == nullptr) {
      this->loadLabelBlock(nLabelBlocks-1);
    }
    labels[nLabelBlocks-1]->insert(value);
    ++nrows;
  }
  blocks[nBlocks-1]->insert(hash.getRow(value));
  ++nnz;
}


FilteredBitVector::FilteredBitVector(Size n_blocks) : Matrix(n_blocks) {
  blocks.reserve(nBlocks);
}

FilteredBitVector::~FilteredBitVector() {
  for (auto bl : blocks) {
    delete bl;
  }
}

void FilteredBitVector::deleteBlock(Size idx) {
  delete blocks[idx];
}


DecimalMap::DecimalMap(Size n_blocks) : Matrix(n_blocks) {
  blocks.reserve(nBlocks);
}

DecimalMap::~DecimalMap() {
  for (auto bl : blocks) {
    delete bl;
  }
}

void DecimalMap::deleteBlock(Size idx) {
  delete blocks[idx];
}


FilteredDecimalVector::FilteredDecimalVector(Size n_blocks) : Matrix(n_blocks) {
  blocks.reserve(nBlocks);
}

FilteredDecimalVector::~FilteredDecimalVector() {
  for (auto bl : blocks) {
    delete bl;
  }
}

void FilteredDecimalVector::deleteBlock(Size idx) {
  delete blocks[idx];
}


FilteredBitmap::FilteredBitmap(Size n_blocks) : Matrix(n_blocks) {
  blocks.reserve(nBlocks);
}

FilteredBitmap::~FilteredBitmap() {
  for (auto bl : blocks) {
    delete bl;
  }
}

void FilteredBitmap::deleteBlock(Size idx) {
  delete blocks[idx];
}


FilteredDecimalMap::FilteredDecimalMap(Size n_blocks) : Matrix(n_blocks) {
  blocks.reserve(nBlocks);
}

FilteredDecimalMap::~FilteredDecimalMap() {
  for (auto bl : blocks) {
    delete bl;
  }
}

void FilteredDecimalMap::deleteBlock(Size idx) {
  delete blocks[idx];
}

}  // namespace engine
