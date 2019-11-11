/*
 * Copyright (c) 2019 Daniel Rodrigues. All rights reserved.
 */
#include <chrono>
#include <iostream>
#include <string>
#include <vector>
#include "../../Vtune_ITT/tracing.h"
#include "../../stream/include/StreamComponents_time.hpp"

#ifndef DATASET
  #define DATASET 4
#endif

#ifndef READ_THREADS
  #define READ_THREADS 4
#endif
#ifndef WORK_THREADS
  #define WORK_THREADS 8
#endif
#ifndef DOT_THREADS
  #define DOT_THREADS 4
#endif
#ifndef HAD_THREADS
  #define HAD_THREADS 2
#endif

inline bool filter_var_a(std::vector<engine::Literal> *args){return (*args)[0]>="1995-09-01"&&(*args)[0]<"1995-10-01";}
inline engine::Decimal lift_var_b(std::vector<engine::Decimal> *args) {return (*args)[0]*(1-(*args)[1]);}
inline bool filter_var_d(std::vector<engine::Literal> *args){return match((*args)[0],"PROMO.*");}
inline engine::Decimal lift_var_i(std::vector<engine::Decimal> args) {return 100.00*(args)[0]/(args)[1];}

int main() {
  string tpch = string("TPCH_") +  std::to_string(DATASET);

  auto start = std::chrono::high_resolution_clock::now();

  engine::Database db("data/la",tpch,false);

  engine::Bitmap *var_lineitem_shipdate =
    new engine::Bitmap(db.data_path,
      db.database_name,
      "lineitem",
      "shipdate");
  engine::DecimalVector *var_lineitem_extendedprice =
    new engine::DecimalVector(db.data_path,
      db.database_name,
      "lineitem",
      "extendedprice");
  engine::Bitmap *var_part_type =
    new engine::Bitmap(db.data_path,
      db.database_name,
      "part",
      "type");

  engine::FilteredBitVector *var_a_pred =
    new engine::FilteredBitVector(var_lineitem_shipdate->nLabelBlocks);
  engine::FilteredBitVector *var_d_pred =
    new engine::FilteredBitVector(var_part_type->nLabelBlocks);
  engine::FilteredBitVector *var_d =
    new engine::FilteredBitVector(var_part_type->nBlocks);

  engine::Decimal *var_g = new engine::Decimal();
  engine::Decimal *var_h = new engine::Decimal();
  engine::Decimal *var_i = new engine::Decimal();

  boost::thread *loader_a_t,*filter_a_t,*loader_d_t,*dot_1_t,
                *filter_d_t,*loader_3_t,*sum_f_t,*sum_c_t,*had_f_t,
                *had_c_t,*dot_3_t,*loader_7_t,*loader_6_t,*loader_5_t,
                *loader_4_t,*map_t,*dot_2_t;

  auto *loader_a = 
    new Load_LabelBlock(READ_THREADS,var_lineitem_shipdate,
                        (var_lineitem_shipdate->nLabelBlocks));
  auto *filter_a = 
    new Filter_LabelBlock(filter_var_a,var_a_pred,WORK_THREADS,loader_a);

  auto *loader_d = 
    new Load_LabelBlock(READ_THREADS,var_part_type,(var_part_type->nLabelBlocks));
  auto *filter_d = 
    new Filter_LabelBlock(filter_var_d,var_d_pred,WORK_THREADS,loader_d);

  auto *loader_3 =
    new Load_BitmapBlock(READ_THREADS,var_part_type,var_part_type->nBlocks);
  auto *dot_1 =
    new Dot_BitmapBlock(var_d_pred,DOT_THREADS,loader_3,var_d);

  auto *load_4 = new Load_BitmapBlock(READ_THREADS,var_lineitem_shipdate,var_lineitem_shipdate->nBlocks);
  
  auto *dot_2 = new Dot_BitmapBlock_2(var_a_pred,DOT_THREADS,load_4);

  auto *load_5 = 
    new Load_DecimalVectorBlock(READ_THREADS,var_lineitem_extendedprice,
                                var_lineitem_shipdate->nBlocks);

  auto *load_6 = 
    new Load_DecimalVectorBlock(READ_THREADS,db.data_path,db.database_name,"lineitem","discount",
                                  var_lineitem_shipdate->nBlocks);

  auto *map = new Map_2(lift_var_b,WORK_THREADS,load_5,load_6);

  auto *had_c = new Hadamard_DecimalVectorBlock_2(HAD_THREADS,dot_2,map);

  auto *sum_f = new Sum(var_h,HAD_THREADS,had_c);

  auto *load_7 = 
    new Load_BitmapBlock(READ_THREADS,db.data_path, db.database_name, "lineitem", "partkey",
                          var_lineitem_shipdate->nBlocks);

  auto *dot_3 = new Dot_BitmapBlock_2(var_d,DOT_THREADS,load_7);

  auto *had_f = new Hadamard_DecimalVectorBlock_3(HAD_THREADS,had_c,dot_3);

  auto *sum_c = new Sum(var_g,HAD_THREADS,had_f);

  loader_a_t = new boost::thread(&Load_LabelBlock::run, loader_a);
  loader_d_t = new boost::thread(&Load_LabelBlock::run, loader_d);
  filter_a_t = new boost::thread(&Filter_LabelBlock::run, filter_a);
  filter_d_t = new boost::thread(&Filter_LabelBlock::run, filter_d);
  loader_3_t = new boost::thread(&Load_BitmapBlock::run, loader_3);
  loader_4_t = new boost::thread(&Load_BitmapBlock::run, load_4);
  loader_5_t = new boost::thread(&Load_DecimalVectorBlock::run, load_5);
  loader_6_t = new boost::thread(&Load_DecimalVectorBlock::run, load_6);
  map_t = new boost::thread(&Map_2::run, map);
  loader_7_t = new boost::thread(&Load_BitmapBlock::run, load_7);

  filter_d_t->join();
  //Wait Filter_D
  dot_1_t = new boost::thread(&Dot_BitmapBlock::run, dot_1);

  dot_1_t->join();
  //DOT
  dot_2_t = new boost::thread(&Dot_BitmapBlock_2::run, dot_2);
  had_f_t = new boost::thread(&Hadamard_DecimalVectorBlock_3::run, had_f);
  had_c_t = new boost::thread(&Hadamard_DecimalVectorBlock_2::run, had_c);

  sum_c_t = new boost::thread(&Sum::run, sum_c);

  filter_a_t->join();
  //Wait Filter_A
  dot_3_t = new boost::thread(&Dot_BitmapBlock_2::run, dot_3);
  sum_f_t = new boost::thread(&Sum::run, sum_f);

  sum_c_t->join();
  sum_f_t->join();

  (*var_i) = lift_var_i(
      {
        *(var_g),
        *(var_h)
      });

  std::cout << (*var_i) << std::endl;
  //std::cout << (*var_g) << " " << (*var_h) << std::endl;

  delete var_d_pred;
  delete var_a_pred;
  delete var_d;
  delete var_g;
  delete var_h;
  delete var_i;

  auto end = std::chrono::high_resolution_clock::now();
  std::cout
    //<< "Completed in "
    << std::chrono::duration_cast<std::chrono::nanoseconds>(end-start).count()
    //<< " ns"
    << std::endl;

  return 0;
}
