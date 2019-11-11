/*
 * Copyright (c) 2018 João Afonso. All rights reserved. 
 * From https://github.com/Hubble83/dbms
 */
#ifndef ENGINE_INCLUDE_BLOCK_H_
#define ENGINE_INCLUDE_BLOCK_H_

#include <fstream>
#include <string>
#include <vector>
#include "types.hpp"

namespace engine {

struct Block {
  Size nnz;

  template <class Archive>
  void serialize(Archive & ar) {
    ar(nnz);
  }
};

struct LabelBlock : public Block {
  std::vector<Literal> labels;

  LabelBlock();

  void load(std::string filePath);
  void save(std::string filePath);

  long bytes(){
    long bytes=0;

    for(auto &value: labels){
      bytes += sizeof(value);
    }

    return (sizeof(Decimal)*labels.size());
  }

  void clean(){
    nnz=0;
  }

  void insert(Literal label);
};

struct DecimalVectorBlock : public Block {
  std::vector<Decimal> values;

  DecimalVectorBlock();

  void load(std::string filePath);
  void save(std::string filePath);

  long bytes(){
    return (sizeof(Decimal)*values.size());
  }

  void clean(){
    nnz=0;
  }

  void insert(Decimal value);
};

struct BitmapBlock : public Block {
  std::vector<MultiPrecision> rows;

  BitmapBlock();

  void load(std::string filePath);
  void save(std::string filePath);

  long bytes(){
    return (sizeof(MultiPrecision)*rows.size());
  }

  void clean(){
    nnz=0;
  }

  void insert(MultiPrecision row);
};

struct FilteredBitVectorBlock : public Block {
  std::vector<MultiPrecision> cols;

  FilteredBitVectorBlock();

  long bytes(){
    return (sizeof(MultiPrecision)*cols.size());
  }

  void clean(){
    nnz=0;
    cols[0] = 0;
  }
};

struct DecimalMapBlock : public Block {
  std::vector<Decimal> values;
  std::vector<MultiPrecision> rows;

  long bytes(){
    return (sizeof(Decimal)*values.size()+sizeof(MultiPrecision)*rows.size());
  }

  DecimalMapBlock();
  void clean(){
    nnz=0;
  }
};

struct FilteredDecimalVectorBlock : public Block {
  std::vector<Decimal> values;
  std::vector<MultiPrecision> cols;

  long bytes(){
    return (sizeof(Decimal)*values.size()+sizeof(MultiPrecision)*cols.size());
  }

  FilteredDecimalVectorBlock();
  void clean(){
    nnz=0;
    cols[0] = 0;
  }
};

struct FilteredBitmapBlock : public Block {
  std::vector<MultiPrecision> rows;
  std::vector<MultiPrecision> cols;

  long bytes(){
    return (sizeof(MultiPrecision)*rows.size()+sizeof(MultiPrecision)*cols.size());
  }

  FilteredBitmapBlock();
  void clean(){
    nnz=0;
    cols[0] = 0;
  }
};

struct FilteredDecimalMapBlock : public Block {
  std::vector<Decimal> values;
  std::vector<MultiPrecision> rows;
  std::vector<MultiPrecision> cols;

  long bytes(){
    return (sizeof(Decimal)*values.size()+sizeof(MultiPrecision)*rows.size()+sizeof(MultiPrecision)*cols.size());
  }

  FilteredDecimalMapBlock();
  void clean(){
    nnz=0;
    cols[0] = 0;
  }
};

}  // namespace engine

#endif  // ENGINE_INCLUDE_BLOCK_H_
