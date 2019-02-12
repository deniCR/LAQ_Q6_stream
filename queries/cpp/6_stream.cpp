/*
 * Copyright (c) 2018 Daniel Rodrigues. All rights reserved.
 */

#include <chrono>
#include <iostream>
#include <string>
#include <vector>
#include "include/StreamComponents.hpp"

#ifndef DATASET
  #define DATASET "1"
#endif

using namespace engine;
using namespace boost;
using namespace stream;
using namespace std;

inline bool filter_var_b(vector<Decimal> args)
    { return args[0]>=0.05&&args[0]<=0.07;}

inline bool filter_var_d(vector<Decimal> args)
    { return args[0]<24;}

inline Decimal lift_var_f(vector<Decimal> args)
    { return args[0]*args[1];}

int main() {
  string tpch = string("TPCH_") + DATASET;

  auto start = std::chrono::high_resolution_clock::now();
  Database db("data/la",tpch,false);

  auto *var_lineitem_shipdate = new Bitmap(
  		db.data_path,db.database_name,"lineitem","shipdate"
  		);
  auto *var_lineitem_discount = new DecimalVector(
  		db.data_path,db.database_name,"lineitem","discount"
  		);
  auto *var_lineitem_quantity = new DecimalVector(
  		db.data_path,db.database_name,"lineitem","quantity"
  		);
  auto *var_lineitem_extendedprice = new DecimalVector(
  		db.data_path,db.database_name,"lineitem","extendedprice"
  		);

  auto *var_a_pred = new FilteredBitVector(var_lineitem_shipdate->nLabelBlocks);
  auto *var_a = new FilteredBitVector(var_lineitem_shipdate->nBlocks);
  auto *var_b = new FilteredBitVector(var_lineitem_discount->nBlocks);
  auto *var_c = new FilteredBitVector(var_a->nBlocks);
  auto *var_d = new FilteredBitVector(var_lineitem_quantity->nBlocks);
  auto *var_e = new FilteredBitVector(var_c->nBlocks);
  auto *var_f = new DecimalVector(var_lineitem_extendedprice->nBlocks);
  auto *var_g = new FilteredDecimalVector(var_e->nBlocks);
  auto *var_h = new Decimal();

  auto *filter_a = new Filter_LabelBlock(var_a_pred,1);
  auto *loader_a1 = new Load_LabelBlock(var_lineitem_shipdate, *filter_a);
  auto *sum = new Sum(var_h,2);
  auto *hadamard_s = new Hadamard_DecimalVectorBlock_2(var_g,1,*sum, "HAD_S");
  auto *hadamard_e = new Hadamard_FilteredBitVectorBlock_2(var_e,1,*hadamard_s, "HAD_E");
  auto *hadamard_d = new Hadamard_FilteredBitVectorBlock_2(var_e,1,*hadamard_e, "HAD_D");
  auto *dot = new Dot_BitmapBlock(var_a_pred,var_a,2,*hadamard_d);
  auto *load_a2 = new Load_BitmapBlock(var_lineitem_shipdate,*dot);
  auto *filter_b = new Filter_DecimalVectorBlock(
                        var_b,*filter_var_b,3,
                        hadamard_d->in_join,hadamard_d->done_join
                        );
  auto *load_be = new Load_DecimalVectorBlock(var_lineitem_discount,*filter_b);
  auto *filter_c = new Filter_DecimalVectorBlock(
                        var_d,*filter_var_d,2,
                        hadamard_e->in_join,hadamard_e->done_join
                        ); //Multi-thread traz problemas ...
  auto *load_c= new Load_DecimalVectorBlock(var_lineitem_quantity,*filter_c);
  auto *map = new Map_2(var_f,2,hadamard_s->in_join,hadamard_s->done_join, "MAP"); //Multi-thread traz problemas ...
  load_be->add_out(map->in_join,map->done_join);
  auto *load_f = new Load_DecimalVectorBlock(var_lineitem_extendedprice,*map);

  boost::thread *loader_a1_t,*filter_a_t,*loader_a2_t,*dot_a_t,
                *loader_be_t,*filter_b_t,*had_d_t,*loader_c_t,*filter_c_t,
                *had_e_t,*loader_f_t,*map_t,*had_s_t,*sum_t;


  loader_a1_t = new boost::thread(&Load_LabelBlock::run_seq, loader_a1);
  filter_a_t = new boost::thread(&Filter_LabelBlock::run, filter_a);
  loader_be_t = new boost::thread(&Load_DecimalVectorBlock::run_seq, load_be);  
  filter_b_t = new boost::thread(&Filter_DecimalVectorBlock::run, filter_b);

  loader_a1_t->join();
  loader_a2_t = new boost::thread(&Load_BitmapBlock::run_seq, load_a2);
  filter_a_t->join();

  dot_a_t = new boost::thread(&Dot_BitmapBlock::run, dot);
  had_d_t = new boost::thread(&Hadamard_FilteredBitVectorBlock_2::run_seq, hadamard_d);
  loader_be_t->join();
  loader_c_t = new boost::thread(&Load_DecimalVectorBlock::run_seq, load_c);
  filter_c_t = new boost::thread(&Filter_DecimalVectorBlock::run, filter_c);
  loader_a2_t->join();
  dot_a_t->join();
  had_e_t = new boost::thread(&Hadamard_FilteredBitVectorBlock_2::run_seq, hadamard_e);
  loader_f_t = new boost::thread(&Load_DecimalVectorBlock::run_seq, load_f);
  filter_b_t->join();
  map_t = new boost::thread(&Map_2::run_seq, map); //unsorted_Map não é thread safe ...
  had_s_t = new boost::thread(&Hadamard_DecimalVectorBlock_2::run_seq, hadamard_s);
  sum_t = new boost::thread(&Sum::run_seq, sum); //Multi-thread traz problemas ...

  had_d_t->join();
  loader_c_t->join();
  filter_c_t->join();
  had_e_t->join();
  loader_f_t->join();
  map_t->join();
  had_s_t->join();
  sum_t->join();

  delete var_a_pred;

  std::cout << (*var_h) << std::endl;

  delete var_a;delete var_b;delete var_c;delete var_d;
  delete var_e;delete var_f;delete var_g;delete var_h;

  auto end = std::chrono::high_resolution_clock::now();
  
  std::cout
	 << std::chrono::duration_cast<std::chrono::nanoseconds>(end-start).count()
   << endl
   ;

  return 0;
}