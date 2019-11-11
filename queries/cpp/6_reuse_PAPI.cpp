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

#if defined(D_PAPI) || defined(D_PAPI_OPS)
	#include "../../papi_counters/papi_omp.h"
  	#define NUM_EVENTS 4
  	int my_events[NUM_EVENTS] = { PAPI_TOT_CYC, PAPI_TOT_INS, PAPI_L1_DCM , PAPI_L3_TCM };
#endif

inline bool filter_var_a(std::vector<engine::Literal> *args)
	{ return (*args)[0]>="1994-01-01"&&(*args)[0]<"1995-01-01";}
inline bool filter_var_b(std::vector<engine::Decimal> *args)
	{ return (*args)[0]>=0.05&&(*args)[0]<=0.07;}
inline bool filter_var_d(std::vector<engine::Decimal> *args)
	{ return (*args)[0]<24;}
inline engine::Decimal lift_var_f(std::vector<engine::Decimal> *args)
	{ return (*args)[0]*(*args)[1];}

#ifndef DATASET
	#define DATASET 1
#endif

#ifndef OMP_NUM_THREADS
	#define OMP_NUM_THREADS 48
#endif

using namespace std;
using namespace engine;

int main() {
  	string tpch = string("TPCH_" + std::to_string(DATASET));

	#ifdef D_PAPI
	   	init_papi(NUM_EVENTS,my_events);
	   	start_papi();
	#endif
	#ifdef D_PAPI_OPS
  		init_papi(NUM_EVENTS,my_events);
  	#endif
 	
  	auto start = std::chrono::high_resolution_clock::now();

  	Database db("data/la",tpch,false);

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
	
	for (engine::Size i = 0; i < var_lineitem_shipdate->nLabelBlocks; ++i) {
		var_lineitem_shipdate->loadLabelBlock(i);
		var_a_pred->blocks[i] = new engine::FilteredBitVectorBlock();
		filter(filter_var_a, var_lineitem_shipdate->labels[i],var_a_pred->blocks[i]);
		var_lineitem_shipdate->deleteLabelBlock(i);
	}

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

  	#pragma omp parallel for schedule(dynamic) firstprivate(init_data) reduction(+:var_h)
  	for(engine::Size i = 0; i < var_lineitem_shipdate->nBlocks; ++i)
  	{
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

			#ifdef D_PAPI_OPS
	  			register_thread(thread_number);
	  		#endif
  		}

		#ifdef D_PAPI_OPS
			if(thread_number==1)
			papi_start_agregate(thread_number);
	  	#endif

		var_lineitem_shipdate->loadBlock(i,lineitem_shipdate_BitmapBlock[thread_number]);

		#ifdef D_PAPI_OPS
			if(thread_number==1)
				papi_stop_agregate(thread_number);
			if(thread_number==11)
				papi_start_agregate(thread_number);
	  	#endif

	 	dot(*var_a_pred, *(lineitem_shipdate_BitmapBlock[thread_number]), var_a_block[thread_number]);

		#ifdef D_PAPI_OPS
			if(thread_number==11)
				papi_stop_agregate(thread_number);
			if(thread_number==2)
				papi_start_agregate(thread_number);
	  	#endif

		var_lineitem_discount->loadBlock(i,lineitem_discount_DecimalVecBlock[thread_number]);

		#ifdef D_PAPI_OPS
			if(thread_number==2)
				papi_stop_agregate(thread_number);
			if(thread_number==6)
				papi_start_agregate(thread_number);
	  	#endif

	 	filter(filter_var_b, lineitem_discount_DecimalVecBlock[thread_number],var_b_block[thread_number]);

		#ifdef D_PAPI_OPS
			if(thread_number==6)
				papi_stop_agregate(thread_number);
			if(thread_number==8)
				papi_start_agregate(thread_number);
	  	#endif

	 	krao(*(var_a_block[thread_number]), *(var_b_block[thread_number]), var_c_block[thread_number]);

		#ifdef D_PAPI_OPS
			if(thread_number==8)
				papi_stop_agregate(thread_number);
			if(thread_number==3)
				papi_start_agregate(thread_number);
	  	#endif

		var_lineitem_quantity->loadBlock(i,lineitem_quantity_DecimalVecBlock[thread_number]);

		#ifdef D_PAPI_OPS
			if(thread_number==3)
				papi_stop_agregate(thread_number);
			if(thread_number==7)
				papi_start_agregate(thread_number);
	  	#endif

	 	filter(filter_var_d,
			lineitem_quantity_DecimalVecBlock[thread_number],
			var_d_block[thread_number]);

		#ifdef D_PAPI_OPS
			if(thread_number==7)
				papi_stop_agregate(thread_number);
			if(thread_number==9)
				papi_start_agregate(thread_number);
	  	#endif

	 	krao(*(var_c_block[thread_number]), *(var_d_block[thread_number]), var_e_block[thread_number]);

		#ifdef D_PAPI_OPS
			if(thread_number==9)
				papi_stop_agregate(thread_number);
			if(thread_number==4)
				papi_start_agregate(thread_number);
	  	#endif

		var_lineitem_extendedprice->loadBlock(i,lineitem_extendedprice_DecimalVecBlock[thread_number]);

		#ifdef D_PAPI_OPS
			if(thread_number==4)
				papi_stop_agregate(thread_number);
			if(thread_number==12)
				papi_start_agregate(thread_number);
	  	#endif

	 	std::vector<DecimalVectorBlock*> aux;
	 	aux.push_back((lineitem_extendedprice_DecimalVecBlock[thread_number]));
	 	aux.push_back((lineitem_discount_DecimalVecBlock[thread_number]));

	 	lift(lift_var_f, &aux, var_f_block[thread_number]);

		#ifdef D_PAPI_OPS
			if(thread_number==12)
				papi_stop_agregate(thread_number);
			if(thread_number==10)
				papi_start_agregate(thread_number);
	  	#endif

	 	krao(*(var_e_block[thread_number]), *(var_f_block[thread_number]), var_g_block[thread_number]);

		#ifdef D_PAPI_OPS
			if(thread_number==10)
				papi_stop_agregate(thread_number);
			if(thread_number==13)
				papi_start_agregate(thread_number);
	  	#endif

	 	var_h+=sum(*var_g_block[thread_number]);

		#ifdef D_PAPI_OPS
			if(thread_number==13)
				papi_stop_agregate(thread_number);
	  	#endif
  	}

  	delete var_a_pred;

  	for(int i=0; i<num_threads; ++i){
  		delete(var_a_block[i]);
  		delete(var_b_block[i]);
  		delete(var_c_block[i]);
  		delete(var_d_block[i]);
  		delete(var_e_block[i]);
  		delete(var_f_block[i]);
  		delete(var_g_block[i]);
  	}

  	std::cout << var_h << std::endl;
  	auto end = std::chrono::high_resolution_clock::now();
  	std::cout
		<< std::chrono::duration_cast<std::chrono::nanoseconds>(end-start).count()
		<< std::endl;

	#ifdef D_PAPI_OPS
		/// PAPI
		//papi_results(num_threads);
		//read_all_agregate_papi();
		print_per_thread_omp();
		/// PAPI
	#endif
	#ifdef D_PAPI
		// PAPI
		//papi_results(num_threads);
		stop_papi();
		//read_all_agregate_papi();
		//print_per_thread_omp();
		// PAPI
	#endif

  return 0;
}