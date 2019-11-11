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
	{return (*args)[0]*(*args)[1];}

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

  	auto start = std::chrono::high_resolution_clock::now();

  	#ifdef D_VTUNE
  		VTuneDomain *vtd = new VTuneDomain("All");
  	#endif

  	#ifdef D_PAPI
	   	init_papi(NUM_EVENTS,my_events);
	   	start_papi();
	#endif
	#ifdef D_PAPI_OPS
  		init_papi(NUM_EVENTS,my_events);
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
  	engine::Decimal var_h = 0;

    #ifdef D_VTUNE
  		vtd->end_task();
  		vtd->start_task("Loop_1");
  	#endif

  	// Load and Filter l_shipdate_label
  	for (engine::Size i = 0; i < var_lineitem_shipdate->nLabelBlocks; ++i)
  	{

	  	#ifdef D_VTUNE
			vtd->start_task("Load1");
		#endif
		var_lineitem_shipdate->loadLabelBlock(i);
	  	#ifdef D_VTUNE
			vtd->end_task();
		#endif

		var_a_pred->blocks[i] = new engine::FilteredBitVectorBlock();

	  	#ifdef D_VTUNE
			vtd->start_task("Filter_1");
		#endif
		filter(filter_var_a,var_lineitem_shipdate->labels[i],var_a_pred->blocks[i]);
	  	#ifdef D_VTUNE
			vtd->end_task();
		#endif

		var_lineitem_shipdate->deleteLabelBlock(i);
  	}

   	#ifdef D_VTUNE
  		vtd->end_task();
		vtd->start_task("Loop_2");
	#endif

  	int init_data=0;

  	#pragma omp parallel for firstprivate(init_data) reduction(+:var_h)
  	for(engine::Size i = 0; i < var_lineitem_shipdate->nBlocks; ++i)
  	{
  		#ifdef D_PAPI_OPS
  		int thread_number = omp_get_thread_num();
	  		if(init_data==0)
	  		{
		  		register_thread(thread_number);
	  			++init_data;
	  		}
	  	#endif

		#ifdef D_PAPI_OPS
			if(thread_number==1)
			papi_start_agregate(thread_number);
	  	#endif
	  	#ifdef D_VTUNE
			vtd->start_task("Load2");
		#endif
		var_lineitem_shipdate->loadBlock(i);
	  	#ifdef D_VTUNE
			vtd->end_task();
		#endif

		var_a->blocks[i] = new engine::FilteredBitVectorBlock();
		// Dot

		#ifdef D_PAPI_OPS
			if(thread_number==1)
				papi_stop_agregate(thread_number);
			if(thread_number==11)
				papi_start_agregate(thread_number);
	  	#endif
	  	#ifdef D_VTUNE
			vtd->start_task("Dot");
		#endif
		dot(*var_a_pred, *(var_lineitem_shipdate->blocks[i]), var_a->blocks[i]);
	  	#ifdef D_VTUNE
			vtd->end_task();
		#endif

		var_lineitem_shipdate->deleteBlock(i);

		#ifdef D_PAPI_OPS
			if(thread_number==11)
				papi_stop_agregate(thread_number);
			if(thread_number==2)
				papi_start_agregate(thread_number);
	  	#endif
	  	#ifdef D_VTUNE
			vtd->start_task("Load3");
		#endif
		var_lineitem_discount->loadBlock(i);
	  	#ifdef D_VTUNE
		 	vtd->end_task();
		#endif

		var_b->blocks[i] = new engine::FilteredBitVectorBlock();

		#ifdef D_PAPI_OPS
			if(thread_number==2)
				papi_stop_agregate(thread_number);
			if(thread_number==6)
				papi_start_agregate(thread_number);
	  	#endif
	  	#ifdef D_VTUNE
			vtd->start_task("Filter_2");
		#endif
		filter(filter_var_b,
	 		var_lineitem_discount->blocks[i],
			var_b->blocks[i]);
	  	#ifdef D_VTUNE
		 	vtd->end_task();
		#endif

		var_c->blocks[i] = new engine::FilteredBitVectorBlock();

		#ifdef D_PAPI_OPS
			if(thread_number==6)
				papi_stop_agregate(thread_number);
			if(thread_number==8)
				papi_start_agregate(thread_number);
	  	#endif
	  	#ifdef D_VTUNE
			vtd->start_task("Had_1");
		#endif
		krao(*(var_a->blocks[i]),
		  		*(var_b->blocks[i]),
		  		var_c->blocks[i]);
	  	#ifdef D_VTUNE
		 	vtd->end_task();
		#endif

		var_a->deleteBlock(i);
		var_b->deleteBlock(i);

		#ifdef D_PAPI_OPS
			if(thread_number==8)
				papi_stop_agregate(thread_number);
			if(thread_number==3)
				papi_start_agregate(thread_number);
	  	#endif
	  	#ifdef D_VTUNE
			vtd->start_task("Load4");
		#endif
		var_lineitem_quantity->loadBlock(i);
	  	#ifdef D_VTUNE
		 	vtd->end_task();
		#endif

		var_d->blocks[i] = new engine::FilteredBitVectorBlock();

		#ifdef D_PAPI_OPS
			if(thread_number==3)
				papi_stop_agregate(thread_number);
			if(thread_number==7)
				papi_start_agregate(thread_number);
	  	#endif
	  	#ifdef D_VTUNE
			vtd->start_task("Filter_3");
		#endif
		filter(filter_var_d,
				var_lineitem_quantity->blocks[i],
				var_d->blocks[i]);
	  	#ifdef D_VTUNE
		 	vtd->end_task();
		#endif

		var_lineitem_quantity->deleteBlock(i);

		var_e->blocks[i] = new engine::FilteredBitVectorBlock();

		#ifdef D_PAPI_OPS
			if(thread_number==7)
				papi_stop_agregate(thread_number);
			if(thread_number==9)
				papi_start_agregate(thread_number);
	  	#endif
	  	#ifdef D_VTUNE
			vtd->start_task("Had_2");
		#endif
		krao(*(var_c->blocks[i]),
			 *(var_d->blocks[i]),
			 var_e->blocks[i]);
	  	#ifdef D_VTUNE
		 	vtd->end_task();
		#endif

		var_c->deleteBlock(i);
		var_d->deleteBlock(i);

		#ifdef D_PAPI_OPS
			if(thread_number==9)
				papi_stop_agregate(thread_number);
			if(thread_number==4)
				papi_start_agregate(thread_number);
	  	#endif
	  	#ifdef D_VTUNE
			vtd->start_task("Load5");
		#endif
		var_lineitem_extendedprice->loadBlock(i);
	  	#ifdef D_VTUNE
		 	vtd->end_task();
		#endif

		var_f->blocks[i] = new engine::DecimalVectorBlock();

		#ifdef D_PAPI_OPS
			if(thread_number==4)
				papi_stop_agregate(thread_number);
			if(thread_number==12)
				papi_start_agregate(thread_number);
	  	#endif
	  	#ifdef D_VTUNE
			vtd->start_task("Map");
		#endif
		std::vector<DecimalVectorBlock*> aux;
		aux.push_back(var_lineitem_extendedprice->blocks[i]);
		aux.push_back(var_lineitem_discount->blocks[i]);
		lift(lift_var_f, &aux, var_f->blocks[i]);
	  	#ifdef D_VTUNE
		 	vtd->end_task();
		#endif

		var_lineitem_extendedprice->deleteBlock(i);
		var_lineitem_discount->deleteBlock(i);

		var_g->blocks[i] = new engine::FilteredDecimalVectorBlock();

		#ifdef D_PAPI_OPS
			if(thread_number==12)
				papi_stop_agregate(thread_number);
			if(thread_number==10)
				papi_start_agregate(thread_number);
	  	#endif
	  	#ifdef D_VTUNE
			vtd->start_task("Had_3");
		#endif
		krao(*(var_e->blocks[i]),
			 *(var_f->blocks[i]),
			 var_g->blocks[i]);
	  	#ifdef D_VTUNE
		 	vtd->end_task();
		#endif

		var_e->deleteBlock(i);
		var_f->deleteBlock(i);

		#ifdef D_PAPI_OPS
			if(thread_number==10)
				papi_stop_agregate(thread_number);
			if(thread_number==13)
				papi_start_agregate(thread_number);
	  	#endif
	  	#ifdef D_VTUNE
			vtd->start_task("Sum");
		#endif
		var_h+=sum(*var_g->blocks[i]);
	  	#ifdef D_VTUNE
		 	vtd->end_task();
		#endif
		var_g->deleteBlock(i);
		#ifdef D_PAPI_OPS
			if(thread_number==13)
				papi_stop_agregate(thread_number);
	  	#endif
  }

	#ifdef D_VTUNE
	 	vtd->end_task();
	#endif

  	delete var_a_pred;

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
