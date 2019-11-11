/*
 * Copyright (c) 2018 João Afonso. All rights reserved. 
 * From https://github.com/Hubble83/dbms
 */
#ifndef ENGINE_INCLUDE_MATRIX_H_
#define ENGINE_INCLUDE_MATRIX_H_

#include <fstream>
#include <map>
#include <string>
#include <vector>
#include "block.hpp"
#include "types.hpp"

namespace engine {

struct Matrix {
  Size nnz;
  Size nrows;
  Size nBlocks;
  Size nLabelBlocks;
  std::string labelsTable;
  std::string labelsAttribute;

  std::string dataPath;
  std::string database;
  std::string table;
  std::string attribute;

  explicit Matrix(Size n_blocks);
  explicit Matrix(const std::string data_path,
                  const std::string database_name,
                  const std::string table_name,
                  const std::string attribute_name);
  explicit Matrix(const std::string data_path,
                  const std::string database_name,
                  const std::string table_name,
                  const std::string attribute_name,
                  const std::string labels_table,
                  const std::string labels_attribute);

  bool save();

 protected:
  std::string getPath();
};

struct DecimalVector : public Matrix {
  std::vector<DecimalVectorBlock*> blocks;
  explicit DecimalVector(Size n_blocks);
  DecimalVector(const std::string data_path,
                const std::string database_name,
                const std::string table_name,
                const std::string attribute_name);
  ~DecimalVector();

  void loadBlock(Size idx);
  void loadBlock(Size idx, DecimalVectorBlock *b);
  void deleteBlock(Size idx);
  void saveBlock(Size idx);
  void saveLastBlock();

  void insert(Decimal value);
};

struct Bitmap : public Matrix {
 private:
  struct LabelHash {
    std::map<Literal, MultiPrecision> labels;
    MultiPrecision nnz;

    void load(std::string attrPath);
    void save(std::string attrPath);

    bool contains(Literal label);
    MultiPrecision getRow(Literal label);
    void insert(Literal label);
  };

 public:
  std::vector<BitmapBlock*> blocks;
  std::vector<LabelBlock*> labels;
  LabelHash hash;

  explicit Bitmap(Size n_blocks);
  Bitmap(const std::string data_path,
         const std::string database_name,
         const std::string table_name,
         const std::string attribute_name);
  Bitmap(const std::string data_path,
         const std::string database_name,
         const std::string table_name,
         const std::string attribute_name,
         const std::string labels_table,
         const std::string labels_attribute);
  ~Bitmap();


  void loadBlock(Size idx);
  void loadBlock(Size idx, BitmapBlock*b);
  void deleteBlock(Size idx);
  void saveBlock(Size idx);
  void saveLastBlock();

  void loadLabelBlock(Size idx);
  void loadLabelBlock(Size idx, LabelBlock *l);
  void deleteLabelBlock(Size idx);
  void saveLabelBlock(Size idx);
  void saveLastLabelBlock();

  void loadLabelHash();
  void saveLabelHash();

  void insert(Literal value);
};

struct FilteredBitVector : public Matrix {
  std::vector<FilteredBitVectorBlock*> blocks;
  explicit FilteredBitVector(Size n_blocks);
  ~FilteredBitVector();
  void deleteBlock(Size idx);
};

struct DecimalMap : public Matrix {
  std::vector<DecimalMapBlock*> blocks;
  explicit DecimalMap(Size n_blocks);
  ~DecimalMap();
  void deleteBlock(Size idx);
};

struct FilteredDecimalVector : public Matrix {
  std::vector<FilteredDecimalVectorBlock*> blocks;
  explicit FilteredDecimalVector(Size n_blocks);
  ~FilteredDecimalVector();
  void deleteBlock(Size idx);
};

struct FilteredBitmap : public Matrix {
  std::vector<FilteredBitmapBlock*> blocks;
  explicit FilteredBitmap(Size n_blocks);
  ~FilteredBitmap();
  void deleteBlock(Size idx);
};

struct FilteredDecimalMap : public Matrix {
  std::vector<FilteredDecimalMapBlock*> blocks;
  explicit FilteredDecimalMap(Size n_blocks);
  ~FilteredDecimalMap();
  void deleteBlock(Size idx);
};

struct FilteredDecimalMapAcc {
  std::map<MultiPrecision,Decimal> map;
  //explicit FilteredDecimalMap(Size n_blocks);
  //~FilteredDecimalMap();
  //void deleteBlock(Size idx);
  FilteredDecimalMap getMatrix();
};

}  // namespace engine

#endif  // ENGINE_INCLUDE_MATRIX_H_
