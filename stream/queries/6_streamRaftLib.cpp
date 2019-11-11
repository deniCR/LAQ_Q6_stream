/*
 * Copyright (c) 2019 Daniel Rodrigues. All rights reserved.
 */
#include <chrono>
#include <iostream>
#include <string>
#include <vector>
#include <raft>

#include "StreamStructRaftLib.hpp"

#ifndef DATASET
  #define DATASET 1
#endif

using namespace engine;

int main() {
  string tpch = string("TPCH_" + std::to_string(DATASET));

  auto start = std::chrono::high_resolution_clock::now();

  Database db("data/la",tpch,false);

  Bitmap *var_lineitem_shipdate = new Bitmap(
      db.data_path,db.database_name,"lineitem","shipdate"
      );
  DecimalVector *var_lineitem_discount = new DecimalVector(
      db.data_path,db.database_name,"lineitem","discount"
      );
  DecimalVector *var_lineitem_quantity = new DecimalVector(
      db.data_path,db.database_name,"lineitem","quantity"
      );
  DecimalVector *var_lineitem_extendedprice = new DecimalVector(
      db.data_path,db.database_name,"lineitem","extendedprice"
      );

  FilteredBitVector *var_a_pred = new FilteredBitVector(var_lineitem_shipdate->nLabelBlocks);
  FilteredBitVector *var_a = new FilteredBitVector(var_lineitem_shipdate->nBlocks);
  FilteredBitVector *var_b = new FilteredBitVector(var_lineitem_discount->nBlocks);
  FilteredBitVector *var_c = new FilteredBitVector(var_a->nBlocks);
  FilteredBitVector *var_d = new FilteredBitVector(var_lineitem_quantity->nBlocks);
  FilteredBitVector *var_e = new FilteredBitVector(var_c->nBlocks);
  DecimalVector *var_f = new DecimalVector(var_lineitem_extendedprice->nBlocks);
  FilteredDecimalVector *var_g = new FilteredDecimalVector(var_e->nBlocks);
  Decimal *var_h = new Decimal();

  Loader_A1 loader_a1(var_lineitem_shipdate);
  Filter_A filter_a(var_a_pred);
  Loader_A2 loader_a2(var_lineitem_shipdate);
  Dot_A dot_a(var_a_pred,var_a);
  Loader_BE loader_be(var_lineitem_discount);
  Filter_B filter_b(var_b);
  Hadamard_D hadamard_d(var_c);
  Loader_C loader_c(var_lineitem_quantity);
  Filter_C filter_c(var_d);
  Hadamard_E hadamard_e(var_e);
  Loader_F loader_f(var_lineitem_extendedprice);
  Map_F map_f(var_f);
  Hadamard_G hadamard_g(var_g);
  Sum sum(var_h);

  raft::map m;

  m += loader_a1 >> filter_a;

  m.exe();

  raft::map m2;

  m2 += loader_a2 >> dot_a >> hadamard_d ["in"] ["out"] >>
      hadamard_e ["in"] ["out"] >> hadamard_g ["in2"] ["out"] >> sum;
  m2 += loader_be ["out"] >> filter_b >> hadamard_d ["in2"];
  m2 += loader_be ["out2"] >> map_f ["in"] >> hadamard_g ["in"];
  m2 += loader_c <= filter_c >> hadamard_e ["in2"];
  m2 += loader_f <= map_f ["in2"];

  m2.exe();

  delete var_a_pred;

  std::cout << (*var_h) << std::endl;

  delete var_h;

  auto end = std::chrono::high_resolution_clock::now();
  std::cout
   //<< "Completed in "
   << std::chrono::duration_cast<std::chrono::nanoseconds>(end-start).count()
   //<< " ns"
   << std::endl;

  return 0;
}
