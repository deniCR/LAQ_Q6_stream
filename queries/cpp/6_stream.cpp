/*
 * Copyright (c) 2018 Daniel Rodrigues. All rights reserved.
 */

#include <chrono>
#include <iostream>
#include <string>
#include <vector>
#include "include/StreamComponents.hpp"

#ifndef DATASET
  #define DATASET "4"
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
  Sum *sum = new Sum(var_h,1);
  auto *hadamard_s = new Hadamard_DecimalVectorBlock(var_g,1,*sum);
  auto *join_s  = new Join< FilteredBitVectorBlock*,
                            DecimalVectorBlock*
                          >(1,*hadamard_s);
  auto *hadamard_e = new Hadamard_FilteredBitVectorBlock(var_e,1,*join_s);
  auto *join_e = new Join < FilteredBitVectorBlock*,
                            FilteredBitVectorBlock*
                          >(1,*hadamard_e);
  auto *hadamard_d = new Hadamard_FilteredBitVectorBlock(var_e,1,*join_e);
  auto *join_d = new Join < FilteredBitVectorBlock*,
                            FilteredBitVectorBlock*
                          >(1,*hadamard_d);
  auto *dot = new Dot_BitmapBlock(var_a_pred,var_a,1,*join_d);
  auto *load_a2 = new Load_BitmapBlock(var_lineitem_shipdate,*dot);
  auto *filter_b = new Filter_DecimalVectorBlock(
                        var_b,*filter_var_b,1,
                        join_d->in_join,join_d->done_join
                        );
  auto *load_be = new Load_DecimalVectorBlock(var_lineitem_discount,*filter_b);
  auto *filter_c = new Filter_DecimalVectorBlock(
                        var_d,*filter_var_d,1,
                        join_e->in_join,join_e->done_join
                        );
  auto *load_c= new Load_DecimalVectorBlock(var_lineitem_quantity,*filter_c);
  Map *map = new Map(var_f,1,join_s->in_join,join_s->done_join);
  auto *join_m = new Join<DecimalVectorBlock*,DecimalVectorBlock*>(1,*map);
  load_be->add_out(join_m->in_join,join_m->done_join);
  auto *load_f = new Load_DecimalVectorBlock(var_lineitem_extendedprice,*join_m);

  boost::thread *loader_a1_t,*filter_a_t,*loader_a2_t,*dot_a_t,
                *loader_be_t,*filter_b_t,*had_d_t,*loader_c_t,*filter_c_t,
                *had_e_t,*loader_f_t,*map_t,*had_s_t,*sum_t,*join_d_t,*join_e_t,
                *join_s_t,*join_m_t;


  loader_a1_t = new boost::thread(&Load_LabelBlock::run_seq, loader_a1);
  filter_a_t = new boost::thread(&Filter_LabelBlock::run_seq, filter_a);
  loader_be_t = new boost::thread(&Load_DecimalVectorBlock::run_seq, load_be);  
  filter_b_t = new boost::thread(&Filter_DecimalVectorBlock::run_seq, filter_b);

  loader_a1_t->join();
  loader_a2_t = new boost::thread(&Load_BitmapBlock::run_seq, load_a2);
  filter_a_t->join();

  dot_a_t = new boost::thread(&Dot_BitmapBlock::run_seq, dot);
  join_d_t = new boost::thread(&Join< FilteredBitVectorBlock*,
                                      FilteredBitVectorBlock*
                                    >::run_seq, join_d);
  had_d_t = new boost::thread(&Hadamard_FilteredBitVectorBlock::run_seq, hadamard_d);
  loader_be_t->join();
  loader_c_t = new boost::thread(&Load_DecimalVectorBlock::run_seq, load_c);
  loader_a2_t->join();
  filter_c_t = new boost::thread(&Filter_DecimalVectorBlock::run_seq, filter_c);
  dot_a_t->join();
  join_e_t = new boost::thread(&Join< FilteredBitVectorBlock*,
                                      FilteredBitVectorBlock*
                                    >::run_seq, join_e);
  had_e_t = new boost::thread(&Hadamard_FilteredBitVectorBlock::run_seq, hadamard_e);
  loader_f_t = new boost::thread(&Load_DecimalVectorBlock::run_seq, load_f);
  filter_b_t->join();
  join_m_t = new boost::thread(&Join< DecimalVectorBlock*,
                                      DecimalVectorBlock*
                                    >::run_seq, join_m);
  map_t = new boost::thread(&Map::run_seq, map);
  join_d_t->join();
  join_s_t = new boost::thread(&Join< FilteredBitVectorBlock*,
                                      DecimalVectorBlock*
                                    >::run_seq, join_s);
  had_s_t = new boost::thread(&Hadamard_DecimalVectorBlock::run_seq, hadamard_s);
  sum_t = new boost::thread(&Sum::run_seq, sum);

  had_d_t->join();
  loader_c_t->join();
  filter_c_t->join();
  join_e_t->join();
  had_e_t->join();
  loader_f_t->join();
  join_m_t->join();
  map_t->join();
  join_s_t->join();
  had_s_t->join();
  sum_t->join();

  delete var_a_pred;

  std::cout << (*var_h) << std::endl;

  delete var_h;

  auto end = std::chrono::high_resolution_clock::now();
  std::cout
	 << std::chrono::duration_cast<std::chrono::nanoseconds>(end-start).count()
   << endl
   ;

  return 0;
}