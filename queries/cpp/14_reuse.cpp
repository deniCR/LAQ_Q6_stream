/*
 * Copyright (c) 2019 Daniel Rodrigues. All rights reserved.
 */
#include <chrono>
#include <iostream>
#include <string>
#include <vector>
#include <omp.h>
#include <block.hpp>
#include <database.hpp>
#include <dot.hpp>
#include <filter.hpp>
#include <fold.hpp>
#include <functions.hpp>
#include <krao.hpp>
#include <lift.hpp>
#include <matrix.hpp>
#include <types.hpp>

#ifndef DATASET
  #define DATASET 1
#endif

#ifndef OMP_NUM_THREADS
  #define OMP_NUM_THREADS 8
#endif

using namespace std;
using namespace engine;

inline bool filter_var_a(std::vector<engine::Literal> *args){return (*args)[0]>="1995-09-01"&&(*args)[0]<"1995-10-01";}
inline engine::Decimal lift_var_b(std::vector<engine::Decimal> *args) {return (*args)[0]*(1-(*args)[1]);}
inline bool filter_var_d(std::vector<engine::Literal> *args){return match((*args)[0],"PROMO.*");}
inline engine::Decimal lift_var_i(std::vector<engine::Decimal> args) {return 100.00*(args)[0]/(args)[1];}

int main() {
  std::string tpch = std::string("TPCH_" + std::to_string(DATASET));

  auto start = std::chrono::high_resolution_clock::now();

  engine::Database db("data/la",tpch,false);

  engine::Bitmap *var_lineitem_shipdate = new engine::Bitmap(db.data_path,db.database_name,"lineitem","shipdate");
  engine::DecimalVector *var_lineitem_extendedprice = new engine::DecimalVector(db.data_path,db.database_name,"lineitem","extendedprice");
  engine::DecimalVector *var_lineitem_discount = new engine::DecimalVector(db.data_path,db.database_name,"lineitem","discount");
  engine::Bitmap *var_part_type = new engine::Bitmap(db.data_path,db.database_name,"part","type");
  engine::Bitmap *var_lineitem_partkey = new engine::Bitmap(db.data_path,db.database_name,"lineitem","partkey");

  engine::FilteredBitVector *var_a_pred = new engine::FilteredBitVector(var_lineitem_shipdate->nLabelBlocks);
  engine::FilteredBitVector *var_a = new engine::FilteredBitVector(var_lineitem_shipdate->nBlocks);
  engine::DecimalVector *var_b = new engine::DecimalVector(var_lineitem_extendedprice->nBlocks);
  engine::FilteredDecimalVector *var_c = new engine::FilteredDecimalVector(var_a->nBlocks);
  engine::FilteredBitVector *var_d_pred = new engine::FilteredBitVector(var_part_type->nLabelBlocks);
  engine::FilteredBitVector *var_d = new engine::FilteredBitVector(var_part_type->nBlocks);
  engine::FilteredBitVector *var_e = new engine::FilteredBitVector(var_lineitem_partkey->nBlocks);
  engine::FilteredDecimalVector *var_f = new engine::FilteredDecimalVector(var_c->nBlocks);
  engine::Decimal var_g = 0;
  engine::Decimal var_h = 0;
  engine::Decimal var_i = 0;

  #pragma omp parallel for
  for (engine::Size i = 0; i < var_lineitem_shipdate->nLabelBlocks; ++i) {
    var_lineitem_shipdate->loadLabelBlock(i);
    var_a_pred->blocks[i] = new engine::FilteredBitVectorBlock();

    filter(filter_var_a,
        var_lineitem_shipdate->labels[i],
      var_a_pred->blocks[i]);

    var_lineitem_shipdate->deleteLabelBlock(i);
  }

  #pragma omp parallel for
  for (engine::Size i = 0; i < var_part_type->nLabelBlocks; ++i) {
    var_part_type->loadLabelBlock(i);
    var_d_pred->blocks[i] = new engine::FilteredBitVectorBlock();

    filter(filter_var_d,
        var_part_type->labels[i],
      var_d_pred->blocks[i]);

    var_part_type->deleteLabelBlock(i);
  }

  #pragma omp parallel for
  for(engine::Size i = 0; i < var_part_type->nBlocks; ++i) {
    var_part_type->loadBlock(i);
    var_d->blocks[i] = new engine::FilteredBitVectorBlock();

    dot(*var_d_pred, *(var_part_type->blocks[i]), var_d->blocks[i]);

    var_part_type->deleteBlock(i);
  }

  delete var_d_pred;

  int num_threads = omp_get_max_threads();

  engine::BitmapBlock *lineitem_shipdate_BitmapBlock[num_threads];
  engine::BitmapBlock *lineitem_partkey_BitmapBlock[num_threads];
  engine::DecimalVectorBlock *lineitem_discount_DecimalVecBlock[num_threads];
  engine::DecimalVectorBlock *lineitem_extendedprice_DecimalVecBlock[num_threads];

  engine::FilteredBitVectorBlock *var_a_block[num_threads];
  engine::DecimalVectorBlock *var_b_block[num_threads];
  engine::FilteredDecimalVectorBlock *var_c_block[num_threads];
  engine::FilteredBitVectorBlock *var_e_block[num_threads];
  engine::FilteredDecimalVectorBlock *var_f_block[num_threads];

  int init_data=0;

  #pragma omp parallel for firstprivate(init_data) reduction(+:var_h) reduction(+:var_g)
  for(engine::Size i = 0; i < var_lineitem_shipdate->nBlocks; ++i) {
    int thread_number = omp_get_thread_num();
    if(init_data==0){
      lineitem_shipdate_BitmapBlock[thread_number] = new engine::BitmapBlock();
      lineitem_partkey_BitmapBlock[thread_number] = new engine::BitmapBlock();
      lineitem_discount_DecimalVecBlock[thread_number] = new engine::DecimalVectorBlock();
      lineitem_extendedprice_DecimalVecBlock[thread_number] = new engine::DecimalVectorBlock();

      var_a_block[thread_number] = new engine::FilteredBitVectorBlock();
      var_b_block[thread_number] = new engine::DecimalVectorBlock();
      var_c_block[thread_number] = new engine::FilteredDecimalVectorBlock();
      var_e_block[thread_number] = new engine::FilteredBitVectorBlock();
      var_f_block[thread_number] = new engine::FilteredDecimalVectorBlock();
    }
    init_data++;

    var_lineitem_shipdate->loadBlock(i,lineitem_shipdate_BitmapBlock[thread_number]);
    //var_a->blocks[i] = new engine::FilteredBitVectorBlock();

    dot(*var_a_pred, *(lineitem_shipdate_BitmapBlock[thread_number]), var_a_block[thread_number]);

    //var_lineitem_shipdate->deleteBlock(i);
    var_lineitem_extendedprice->loadBlock(i,lineitem_extendedprice_DecimalVecBlock[thread_number]);
    var_lineitem_discount->loadBlock(i,lineitem_discount_DecimalVecBlock[thread_number]);
    //var_b->blocks[i] = new engine::DecimalVectorBlock();

    std::vector<DecimalVectorBlock*> aux;
    aux.push_back((lineitem_extendedprice_DecimalVecBlock[thread_number]));
    aux.push_back((lineitem_discount_DecimalVecBlock[thread_number]));

    lift(lift_var_b,
      &aux,
      var_b_block[thread_number]);

    //var_lineitem_extendedprice->deleteBlock(i);
    //var_lineitem_discount->deleteBlock(i);

    //var_c->blocks[i] = new engine::FilteredDecimalVectorBlock();

    krao(*(var_a_block[thread_number]), *(var_b_block[thread_number]), var_c_block[thread_number]);

    //var_a->deleteBlock(i);
    //var_b->deleteBlock(i);

    var_lineitem_partkey->loadBlock(i,lineitem_partkey_BitmapBlock[thread_number]);
    //var_e->blocks[i] = new engine::FilteredBitVectorBlock();

    dot(*var_d, *(lineitem_partkey_BitmapBlock[thread_number]), var_e_block[thread_number]);

    //var_lineitem_partkey->deleteBlock(i);

    //var_f->blocks[i] = new engine::FilteredDecimalVectorBlock();

    krao(*(var_c_block[thread_number]), *(var_e_block[thread_number]), var_f_block[thread_number]);

    //var_e->deleteBlock(i);

    engine::Decimal var_h_tmp=0;
    engine::Decimal var_g_tmp=0;

    sum(*var_f_block[thread_number], &var_g_tmp);

    var_g+=(var_g_tmp);

    //var_f->deleteBlock(i);

    sum(*var_c_block[thread_number], &var_h_tmp);

    var_h+=(var_h_tmp);

    //var_c->deleteBlock(i);
  }

  delete var_a_pred;
  delete var_d;

  var_i = lift_var_i(
      {
        var_g,
        var_h
      });

  std::cout << var_i << std::endl;
  //std::cout << (var_g) << " " << (var_h) << std::endl;

  auto end = std::chrono::high_resolution_clock::now();
  std::cout
    //<< "Completed in "
    << std::chrono::duration_cast<std::chrono::nanoseconds>(end-start).count()
    //<< " ns"
    << std::endl;

  return 0;
}
