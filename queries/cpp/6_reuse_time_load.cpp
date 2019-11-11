/*
 * Copyright (c) 2018 Daniel Rodrigues. All rights reserved.
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

inline bool filter_var_a(std::vector<engine::Literal> *args)
	{ return (*args)[0]>="1994-01-01"&&(*args)[0]<"1995-01-01";}
inline bool filter_var_b(std::vector<engine::Decimal> *args)
	{ return (*args)[0]>=0.05&&(*args)[0]<=0.07;}
inline bool filter_var_d(std::vector<engine::Decimal> *args)
	{ return (*args)[0]<24;}
inline engine::Decimal lift_var_f(std::vector<engine::Decimal> *args)
	{return (*args)[0]*(*args)[1];}

#ifndef DATASET
	#define DATASET 32
#endif

#ifndef OMP_NUM_THREADS
	#define OMP_NUM_THREADS 8
#endif

using namespace std;
using namespace engine;

int main() {
  string tpch = string("TPCH_" + std::to_string(DATASET));

  long b_load1=0,b_load2=0,b_load3=0,b_load4=0,b_load5=0;

  std::chrono::high_resolution_clock::time_point s_load1,s_load2,s_load3,
  												s_load4, s_load5, e_load1,
  												e_load2,e_load3, e_load4, e_load5,
  												s_had1,e_had1,s_had2,e_had2,s_had3,e_had3,
  												s_filter1,e_filter1,s_filter2,e_filter2,s_filter3,e_filter3,
  												s_dot,e_dot,s_map,e_map,s_sum,e_sum;


  auto start = std::chrono::high_resolution_clock::now();
  Database db("data/la",tpch,false);

  auto s_init = std::chrono::high_resolution_clock::now();

  engine::Bitmap *var_lineitem_shipdate = 
  	new engine::Bitmap(db.data_path,db.database_name,"lineitem","shipdate");

  engine::DecimalVector *var_lineitem_discount = 
  	new engine::DecimalVector(db.data_path,db.database_name,"lineitem","discount");
  engine::DecimalVector *var_lineitem_quantity = 
  	new engine::DecimalVector(db.data_path,db.database_name,"lineitem","quantity");
  engine::DecimalVector *var_lineitem_extendedprice = 
  	new engine::DecimalVector(db.data_path,db.database_name,"lineitem","extendedprice");

  engine::FilteredBitVector *var_a_pred = 
  	new engine::FilteredBitVector(var_lineitem_shipdate->nLabelBlocks);
  engine::FilteredBitVector *var_a = 
  	new engine::FilteredBitVector(var_lineitem_shipdate->nBlocks);
  engine::FilteredBitVector *var_b = 
  	new engine::FilteredBitVector(var_lineitem_discount->nBlocks);
  engine::FilteredBitVector *var_c = 
  	new engine::FilteredBitVector(var_a->nBlocks);
  engine::FilteredBitVector *var_d = 
  	new engine::FilteredBitVector(var_lineitem_quantity->nBlocks);
  engine::FilteredBitVector *var_e = 
  	new engine::FilteredBitVector(var_c->nBlocks);
  engine::DecimalVector *var_f = 
  	new engine::DecimalVector(var_lineitem_extendedprice->nBlocks);
  engine::FilteredDecimalVector *var_g = 
  	new engine::FilteredDecimalVector(var_e->nBlocks);
  engine::Decimal var_h=0;

  auto e_init = std::chrono::high_resolution_clock::now();

  // Load and Filter l_shipdate_label
  for (engine::Size i = 0; i < var_lineitem_shipdate->nLabelBlocks; ++i) {
	 
	 s_load1 = std::chrono::high_resolution_clock::now();
	 	var_lineitem_shipdate->loadLabelBlock(i);
	 e_load1 = std::chrono::high_resolution_clock::now();

	 //b_load1 += var_lineitem_shipdate->labels[i]->bytes();

	 var_a_pred->blocks[i] = new engine::FilteredBitVectorBlock();

	 s_filter1 = std::chrono::high_resolution_clock::now();
	 	filter(filter_var_a,{*(var_lineitem_shipdate->labels[i])},var_a_pred->blocks[i]);
	 var_lineitem_shipdate->deleteLabelBlock(i);
	 e_filter1 = std::chrono::high_resolution_clock::now();
  }

  long long init_t = std::chrono::duration_cast<std::chrono::nanoseconds>(e_init - s_init).count();
  long long load_t = std::chrono::duration_cast<std::chrono::nanoseconds>(e_load1 - s_load1).count();
  long long ops_t = std::chrono::duration_cast<std::chrono::nanoseconds>(e_filter1 - s_filter1).count();

  long long t_load1=load_t,t_load2=0,t_load3=0,t_load4=0,t_load5=0,
  			t_had1=0,t_had2=0,t_had3=0,t_filter1=ops_t,t_filter2=0,t_filter3=0,
  			t_dot=0,t_map=0,t_sum=0;

  int num_threads = omp_get_max_threads();

  engine::BitmapBlock *lineitem_shipdate_BitmapBlock[num_threads];
  engine::DecimalVectorBlock *lineitem_discount_DecimalVecBlock[num_threads];
  engine::DecimalVectorBlock *lineitem_quantity_DecimalVecBlock[num_threads];
  engine::DecimalVectorBlock *lineitem_extendedprice_DecimalVecBlock[num_threads];

  engine::FilteredBitVectorBlock *var_a_block[num_threads];
  engine::FilteredBitVectorBlock *var_b_block[num_threads];
  engine::FilteredBitVectorBlock *var_c_block[num_threads];
  engine::FilteredBitVectorBlock *var_d_block[num_threads];
  engine::FilteredBitVectorBlock *var_e_block[num_threads];
  engine::DecimalVectorBlock *var_f_block[num_threads];
  engine::FilteredDecimalVectorBlock *var_g_block[num_threads];

  int init_data=0;



  // Load l_shipdate and dot with l_shipdate_label ... private(s_load2,s_load3,s_load4, s_load5,e_load2,e_load3, e_load4, e_load5) reduction(+:load_t)
  #pragma omp parallel for schedule(dynamic) firstprivate(init_data) private(s_load2,s_load3,s_load4, s_load5,e_load2,e_load3, e_load4, e_load5,s_had1,e_had1,s_had2,e_had2,s_had3,e_had3,s_filter2,e_filter2,s_filter3,e_filter3,s_dot,e_dot,s_map,e_map,s_sum,e_sum) reduction(+:var_h) reduction(+:t_load2) reduction(+:t_load3) reduction(+:t_load4) reduction(+:t_load5) reduction(+:t_had1) reduction(+:t_had2) reduction(+:t_had3) reduction(+:t_filter2) reduction(+:t_filter3) reduction(+:t_dot) reduction(+:t_map) reduction(+:t_sum)
  for(engine::Size i = 0; i < var_lineitem_shipdate->nBlocks; ++i) {
  	int thread_number = omp_get_thread_num();
  	if(init_data==0)
  	{
  	  lineitem_shipdate_BitmapBlock[thread_number] = new engine::BitmapBlock();
  	  lineitem_discount_DecimalVecBlock[thread_number] = new engine::DecimalVectorBlock();
  	  lineitem_quantity_DecimalVecBlock[thread_number] = new engine::DecimalVectorBlock();
  	  lineitem_extendedprice_DecimalVecBlock[thread_number] = new engine::DecimalVectorBlock();

	  var_a_block[thread_number] = new engine::FilteredBitVectorBlock();
	  var_b_block[thread_number] = new engine::FilteredBitVectorBlock();
	  var_c_block[thread_number] = new engine::FilteredBitVectorBlock();
	  var_d_block[thread_number] = new engine::FilteredBitVectorBlock();
	  var_e_block[thread_number] = new engine::FilteredBitVectorBlock();
	  var_f_block[thread_number] = new engine::DecimalVectorBlock();
	  var_g_block[thread_number] = new engine::FilteredDecimalVectorBlock();
	  ++init_data;
  	}

  	 //LOAD_2
	 s_load2 = std::chrono::high_resolution_clock::now();
	 	var_lineitem_shipdate->loadBlock(i,lineitem_shipdate_BitmapBlock[thread_number]);
	 e_load2 = std::chrono::high_resolution_clock::now();
	 t_load2 += std::chrono::duration_cast<std::chrono::nanoseconds>(e_load2 - s_load2).count();

	 //b_load2 += lineitem_shipdate_BitmapBlock[thread_number]->bytes();

	 var_a_block[thread_number]->clean();

	 // Dot
	 s_dot = std::chrono::high_resolution_clock::now();
	 	dot(*var_a_pred, *(lineitem_shipdate_BitmapBlock[thread_number]), var_a_block[thread_number]);
	 //var_lineitem_shipdate->deleteBlock(i);
	 e_dot = std::chrono::high_resolution_clock::now();
	 t_dot += std::chrono::duration_cast<std::chrono::nanoseconds>(e_dot - s_dot).count();

	 //LOAD_3
	 s_load3 = std::chrono::high_resolution_clock::now();
	 	var_lineitem_discount->loadBlock(i,lineitem_discount_DecimalVecBlock[thread_number]);
	 e_load3 = std::chrono::high_resolution_clock::now();
	 t_load3 += std::chrono::duration_cast<std::chrono::nanoseconds>(e_load3 - s_load3).count();

	 //b_load3 += lineitem_discount_DecimalVecBlock[thread_number]->bytes();

	 var_b_block[thread_number]->clean();

	 //FILTER_2
	 s_filter2 = std::chrono::high_resolution_clock::now();
	 	filter(filter_var_b,
			lineitem_discount_DecimalVecBlock[thread_number],
			var_b_block[thread_number]);
	 e_filter2 = std::chrono::high_resolution_clock::now();
	 t_filter2 += std::chrono::duration_cast<std::chrono::nanoseconds>(e_filter2 - s_filter2).count();

	 var_c_block[thread_number]->clean();

	 //HAD_1
	 s_had1 = std::chrono::high_resolution_clock::now();
	 	krao(*(var_a_block[thread_number]),
	 		  *(var_b_block[thread_number]),
	 		  var_c_block[thread_number]);
	 e_had1 = std::chrono::high_resolution_clock::now();
	 t_had1 += std::chrono::duration_cast<std::chrono::nanoseconds>(e_had1 - s_had1).count();

	 //LOAD_4
	 s_load4 = std::chrono::high_resolution_clock::now();
	 	var_lineitem_quantity->loadBlock(i,lineitem_quantity_DecimalVecBlock[thread_number]);
	 e_load4 = std::chrono::high_resolution_clock::now();
	 t_load4 += std::chrono::duration_cast<std::chrono::nanoseconds>(e_load4 - s_load4).count();

	 //b_load4 += lineitem_quantity_DecimalVecBlock[thread_number]->bytes();

	 var_d_block[thread_number]->clean();

	 //FILTER_3
	 s_filter3 = std::chrono::high_resolution_clock::now();
	 	filter(filter_var_d,
			lineitem_quantity_DecimalVecBlock[thread_number],
			var_d_block[thread_number]);
	 //var_lineitem_quantity->deleteBlock(i);
	 e_filter3 = std::chrono::high_resolution_clock::now();
	 t_filter3 += std::chrono::duration_cast<std::chrono::nanoseconds>(e_filter3 - s_filter3).count();

	 var_e_block[thread_number]->clean();

	 //HAD_2
	 s_had2 = std::chrono::high_resolution_clock::now();
	 	krao(*(var_c_block[thread_number]),
	 		*(var_d_block[thread_number]),
	 		var_e_block[thread_number]);
	 e_had2 = std::chrono::high_resolution_clock::now();
	 t_had2 += std::chrono::duration_cast<std::chrono::nanoseconds>(e_had2 - s_had2).count();

	 //LOAD_5
	 s_load5 = std::chrono::high_resolution_clock::now();
	 	var_lineitem_extendedprice->loadBlock(i,lineitem_extendedprice_DecimalVecBlock[thread_number]);
	 e_load5 = std::chrono::high_resolution_clock::now();
	 t_load5 += std::chrono::duration_cast<std::chrono::nanoseconds>(e_load5 - s_load5).count();

	 //b_load5 += lineitem_extendedprice_DecimalVecBlock[thread_number]->bytes();

	 var_f_block[thread_number]->clean();
	 std::vector<DecimalVectorBlock*> aux;
	 aux.push_back((lineitem_extendedprice_DecimalVecBlock[thread_number]));
	 aux.push_back((lineitem_discount_DecimalVecBlock[thread_number]));

	 //MAP
	 s_map = std::chrono::high_resolution_clock::now();
	 	lift(lift_var_f,
			&aux,
			var_f_block[thread_number]);
	 //var_lineitem_extendedprice->deleteBlock(i);
	 //var_lineitem_discount->deleteBlock(i);
	 e_map = std::chrono::high_resolution_clock::now();
	 t_map += std::chrono::duration_cast<std::chrono::nanoseconds>(e_map - s_map).count();

	 var_g_block[thread_number]->clean();

	 //HAD_3
	 s_had3 = std::chrono::high_resolution_clock::now();
	 	krao(*(var_e_block[thread_number]),
	 		*(var_f_block[thread_number]),
	 		var_g_block[thread_number]);
	 e_had3 = std::chrono::high_resolution_clock::now();
	 t_had3 += std::chrono::duration_cast<std::chrono::nanoseconds>(e_had3 - s_had3).count();
	
     engine::Decimal var_h_tmp=0;

     //SUM
     s_sum = std::chrono::high_resolution_clock::now();
	 	sum(*var_g_block[thread_number], &var_h_tmp);
	 e_sum = std::chrono::high_resolution_clock::now();
	 t_sum += std::chrono::duration_cast<std::chrono::nanoseconds>(e_sum - s_sum).count();

	 var_h+=(var_h_tmp);
  }

  delete var_a_pred;

  std::cout << var_h << std::endl;

  auto end = std::chrono::high_resolution_clock::now();

  load_t += t_load2 + t_load3 + t_load4 + t_load5;
  ops_t += t_had1 + t_had2 + t_had3 + t_filter2 + t_filter3 + t_dot + t_map + t_sum;

  //Texec,Tinit,Tload,Tload/thread,TOps,Tops/Thread,T/loads,T/ops...
  std::cout
	 << std::chrono::duration_cast<std::chrono::nanoseconds>(end-start).count() << "\n"
	 << init_t << "\n" << load_t << "\n" << (double)load_t/num_threads << "\n" << ops_t << "\n" << (double)ops_t/num_threads << "\n"
	 << t_load1 << "\n" << t_load2 << "\n" << t_load3 << "\n" << t_load4 << "\n" << t_load5 << "\n"
	 << t_had1 << "\n" << t_had2 << "\n" << t_had3 << "\n" << t_filter1 << "\n" << t_filter2 << "\n" << t_filter3 << "\n"
	 << t_dot << "\n" << t_map << "\n" << t_sum << "\n\n"
	 << b_load1 << "\n" << b_load2 << "\n" << b_load3 << "\n" << b_load4 << "\n" << b_load5
	 << std::endl;

	//std::cout << "#Blocks: " << var_lineitem_shipdate->nBlocks << " Block_size: " << BSIZE << std::endl;

  return 0;
}