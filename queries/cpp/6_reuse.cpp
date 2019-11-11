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

#ifdef D_VTUNE
	#include "../../Vtune_ITT/tracing.h"
	using namespace vtune_tracing;
#endif

#ifndef DATASET
	#define DATASET 1
#endif

#ifndef OMP_NUM_THREADS
	#define OMP_NUM_THREADS 48
#endif

inline bool filter_var_a(std::vector<engine::Literal> *args)
	{ return (*args)[0]>="1994-01-01"&&(*args)[0]<"1995-01-01";}
inline bool filter_var_b(std::vector<engine::Decimal> *args)
	{ return (*args)[0]>=0.05&&(*args)[0]<=0.07;}
inline bool filter_var_d(std::vector<engine::Decimal> *args)
	{ return (*args)[0]<24;}
inline engine::Decimal lift_var_f(std::vector<engine::Decimal> *args)
	{ return (*args)[0]*(*args)[1];}

using namespace std;
using namespace engine;


int main() {
  string tpch = string("TPCH_" + std::to_string(DATASET));

  auto start = std::chrono::high_resolution_clock::now();

  #ifdef D_VTUNE
  	VTuneDomain *vtd = new VTuneDomain("All");
  #endif

  Database db("data/la",tpch,false);

  #ifdef D_VTUNE
  	vtd->start_task("Init1");
  	vtd->start_task("var");
  #endif

  engine::Bitmap *var_lineitem_shipdate = 
  	new engine::Bitmap(db.data_path,db.database_name,"lineitem","shipdate");

  #ifdef D_VTUNE
  	vtd->end_task();
  #endif

  engine::DecimalVector *var_lineitem_discount = 
  	new engine::DecimalVector(db.data_path,db.database_name,"lineitem","discount");
  engine::DecimalVector *var_lineitem_quantity = 
  	new engine::DecimalVector(db.data_path,db.database_name,"lineitem","quantity");
  engine::DecimalVector *var_lineitem_extendedprice = 
  	new engine::DecimalVector(db.data_path,db.database_name,"lineitem","extendedprice");

  	#ifdef D_VTUNE
  		vtd->end_task();
  		vtd->start_task("Init2");
  	#endif

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

  	#ifdef D_LOAD_TIMES
  		std::chrono::high_resolution_clock::time_point s_load1,s_load2,s_load3,
  												s_load4, s_load5, e_load1,
  												e_load2,e_load3, e_load4, e_load5;
  	#endif

    #ifdef D_VTUNE
  		vtd->end_task();
  		vtd->start_task("Loop_1");
  	#endif

  	// Load and Filter l_shipdate_label
  	for (engine::Size i = 0; i < var_lineitem_shipdate->nLabelBlocks; ++i) {		 
	  	#ifdef D_LOAD_TIMES
			s_load1 = std::chrono::high_resolution_clock::now();
		#endif

	  	#ifdef D_VTUNE
			vtd->start_task("Load1");
		#endif
		var_lineitem_shipdate->loadLabelBlock(i);
	  	#ifdef D_VTUNE
			vtd->end_task();
		#endif

	  	#ifdef D_LOAD_TIMES
			e_load1 = std::chrono::high_resolution_clock::now();
		#endif

		var_a_pred->blocks[i] = new engine::FilteredBitVectorBlock();

	  	#ifdef D_VTUNE
			vtd->start_task("Filter_1");
		#endif

		filter(filter_var_a,var_lineitem_shipdate->labels[i],var_a_pred->blocks[i]); 	//Construção da matriz var_a_pred
	  	
	  	#ifdef D_VTUNE
			vtd->end_task();
		#endif

		 var_lineitem_shipdate->deleteLabelBlock(i);
  	}

	#ifdef D_LOAD_TIMES
		long long load1_t = std::chrono::duration_cast<std::chrono::nanoseconds>(e_load1 - s_load1).count();
  		long long load_t = load1_t;
  	#endif

  	#ifdef D_VTUNE
  		vtd->end_task();
		vtd->start_task("Loop_2");
	#endif

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

	#ifdef D_LOAD_TIMES
  		#pragma omp parallel for schedule(dynamic) firstprivate(init_data) reduction(+:var_h) private(s_load2,s_load3,s_load4, s_load5,e_load2,e_load3, e_load4, e_load5) reduction(+:load_t)
  	#else
  		#pragma omp parallel for schedule(dynamic) firstprivate(init_data) reduction(+:var_h)
	#endif
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
		
		#ifdef D_LOAD_TIMES
			s_load2 = std::chrono::high_resolution_clock::now();
		#endif

	  	#ifdef D_VTUNE
			vtd->start_task("Load2");
		#endif

		 var_lineitem_shipdate->loadBlock(i,lineitem_shipdate_BitmapBlock[thread_number]);

	  	#ifdef D_VTUNE
			vtd->end_task();
			vtd->start_task("Dot");
		#endif

		#ifdef D_LOAD_TIMES
			e_load2 = std::chrono::high_resolution_clock::now();
		#endif

		dot(*var_a_pred, *(var_lineitem_shipdate->blocks[i]), var_a_block[thread_number]);

	  	#ifdef D_VTUNE
			vtd->end_task();
			vtd->start_task("Load3");
		#endif

		#ifdef D_LOAD_TIMES
			s_load3 = std::chrono::high_resolution_clock::now();
		#endif

	 	var_lineitem_discount->loadBlock(i,lineitem_discount_DecimalVecBlock[thread_number]);

	  	#ifdef D_VTUNE
			vtd->end_task();
			vtd->start_task("Filter_2");
		#endif

		#ifdef D_LOAD_TIMES
			e_load3 = std::chrono::high_resolution_clock::now();
		#endif

		std::vector<engine::DecimalVectorBlock*> aux2;
		aux2.push_back(var_lineitem_discount->blocks[i]);
		filter(filter_var_b, &aux2, var_b_block[thread_number]);

	  	#ifdef D_VTUNE
		 	vtd->end_task();
			vtd->start_task("Had_1");
		#endif

		 krao(*(var_a_block[thread_number]),
		 	  *(var_b_block[thread_number]),
		 	  var_c_block[thread_number]
		 	 );

	  	#ifdef D_VTUNE
			vtd->end_task();
		 	vtd->start_task("Load4");
		#endif

		#ifdef D_LOAD_TIMES
			s_load4 = std::chrono::high_resolution_clock::now();
		#endif

	 	var_lineitem_quantity->loadBlock(i,lineitem_quantity_DecimalVecBlock[thread_number]);
		
	  	#ifdef D_VTUNE
			vtd->end_task();
			vtd->start_task("Filter_3");
		#endif

		#ifdef D_LOAD_TIMES
			e_load4 = std::chrono::high_resolution_clock::now();
		#endif

		 std::vector<engine::DecimalVectorBlock*> aux3;
		 aux3.push_back(var_lineitem_quantity->blocks[i]);
		 filter(filter_var_d, &aux3, var_d_block[thread_number]);
		
	  	#ifdef D_VTUNE
			vtd->end_task();
			vtd->start_task("Had_2");
		#endif

		krao(*(var_c_block[thread_number]), *(var_d_block[thread_number]), var_e_block[thread_number]);
		
	  	#ifdef D_VTUNE
			vtd->end_task();
			vtd->start_task("Load5");
	  	#endif

		#ifdef D_LOAD_TIMES
			s_load5 = std::chrono::high_resolution_clock::now();
		#endif

	 	var_lineitem_extendedprice->loadBlock(i,lineitem_extendedprice_DecimalVecBlock[thread_number]);

	  	#ifdef D_VTUNE
			vtd->end_task();
			vtd->start_task("Map");
		#endif

		#ifdef D_LOAD_TIMES
			e_load5 = std::chrono::high_resolution_clock::now();
		#endif

		std::vector<DecimalVectorBlock*> aux;
		aux.push_back(var_lineitem_extendedprice->blocks[i]);
		aux.push_back(var_lineitem_discount->blocks[i]);
		lift(lift_var_f, &aux, var_f_block[thread_number]);

	  	#ifdef D_VTUNE
			vtd->end_task();
			vtd->start_task("Had_3");
		#endif

		 krao(*(var_e_block[thread_number]), *(var_f_block[thread_number]), var_g_block[thread_number]);
		 
	  	#ifdef D_VTUNE
			vtd->end_task();
	    	vtd->start_task("Sum");
	    #endif

		var_h+=sum(*var_g_block[thread_number]);

	  	#ifdef D_VTUNE
	    	vtd->end_task();
	    #endif

		#ifdef D_LOAD_TIMES
		 	 load_t += std::chrono::duration_cast<std::chrono::nanoseconds>(
		 	 			(e_load5 - s_load5) + (e_load4 - s_load4) +
		 	 			(e_load3 - s_load3) + (e_load2 - s_load2)).count();
	 	#endif
  }

  for(int i=0; i<num_threads; ++i){
  	delete(var_a_block[i]);
  	delete(var_b_block[i]);
  	delete(var_c_block[i]);
  	delete(var_d_block[i]);
  	delete(var_e_block[i]);
  	delete(var_f_block[i]);
  	delete(var_g_block[i]);
  }

	#ifdef D_LOAD_TIMES
  		vtd->end_task();
  	#endif

  	delete var_a_pred;

  	std::cout << var_h << std::endl;

  	auto end = std::chrono::high_resolution_clock::now();

  	std::cout << std::chrono::duration_cast<std::chrono::nanoseconds>(end-start).count() << std::endl;

  	#ifdef D_LOAD_TIMES
		long long load2_t = std::chrono::duration_cast<std::chrono::nanoseconds>(e_load2 - s_load2).count();
		long long load3_t = std::chrono::duration_cast<std::chrono::nanoseconds>(e_load3 - s_load3).count();
		long long load4_t = std::chrono::duration_cast<std::chrono::nanoseconds>(e_load4 - s_load4).count();
		long long load5_t = std::chrono::duration_cast<std::chrono::nanoseconds>(e_load5 - s_load5).count();
  		std::cout << load_t << ";"
  				<< load1_t << ";"
  				<< load2_t << ";"
  				<< load3_t << ";"
  				<< load4_t << ";"
  				<< load5_t << ";" << std::endl;
  	#endif

  return 0;
}