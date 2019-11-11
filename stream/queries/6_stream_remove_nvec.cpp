/*
 * Copyright (c) 2019 Daniel Rodrigues. All rights reserved.
 */
#include <chrono>
#include <iostream>
#include <string>
#include <vector>

#include "../../stream/include/StreamComponents_remove_novec.hpp"

#if defined(D_PAPI) || defined(D_PAPI_OPS)
  #include "../../papi_counters/papi_stream.h"
  //#define NUM_EVENTS 4
  //char *my_events[NUM_EVENTS] = { "LAST_LEVEL_CACHE_MISSES","OFFCORE_RESPONSE_1:DMND_DATA_RD","OFFCORE_RESPONSE_1:PF_DATA_RD","OFFCORE_RESPONSE_1:PF_LLC_DATA_RD"};
  #define NUM_EVENTS 4
  int my_events[NUM_EVENTS] = { PAPI_TOT_CYC, PAPI_TOT_INS, PAPI_L1_DCM , PAPI_L3_TCM };
  //#define NUM_EVENTS 2
  //int my_events[NUM_EVENTS] = { PAPI_TOT_CYC, PAPI_L1_DCM , PAPI_L2_DCM };
  //int my_events[NUM_EVENTS] = { PAPI_LD_INS, PAPI_SR_INS };
  //#define NUM_EVENTS 2
  //int my_events[NUM_EVENTS] = { PAPI_L1_DCM, PAPI_L3_TCM };
#endif

inline bool filter_var_a(vector<engine::Literal> args)
    { return args[0]>="1994-01-01"&&args[0]<"1995-01-01";}

inline bool filter_var_b(vector<engine::Decimal> args)
    { return args[0]>=0.05&&args[0]<=0.07;}

inline bool filter_var_d(vector<engine::Decimal> args)
    { return args[0]<24;}

inline Decimal lift_var_f_map(vector<engine::Decimal> args)
    { return args[0]*args[1];}

#ifndef DATASET
  #define DATASET 32
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

using namespace engine;
using namespace boost;
using namespace stream;
using namespace std;

int main() {
  string tpch = string("TPCH_") +  std::to_string(DATASET);

  #if D_PAPI
    init_papi(NUM_EVENTS,my_events);
    //int th_id = start_thread();
  #endif

  auto start = std::chrono::high_resolution_clock::now();
  Database db("data/la",tpch,false);

  auto *var_lineitem_shipdate = new Bitmap(
      db.data_path,db.database_name,"lineitem","shipdate"
      );

  boost::thread *loader_a1_t,*filter_a_t,*loader_a2_t,*dot_a_t,
                *loader_be_t,*filter_b_t,*had_d_t,*loader_c_t,*filter_c_t,
                *had_e_t,*loader_f_t,*map_t,*had_s_t,*sum_t;

  auto *var_a_pred = 
    new FilteredBitVector(var_lineitem_shipdate->nLabelBlocks);
  auto *var_h = new Decimal();

  #if defined(D_PAPI) || defined(D_PAPI_OPS)

	  auto *loader_a1 = 
	    new Load_LabelBlock(1,var_lineitem_shipdate,
	                        var_lineitem_shipdate->nBlocks,0);
	  loader_a1_t = new boost::thread(&Load_LabelBlock::run_seq, loader_a1);

	  auto *filter_a = 
	    new Filter_LabelBlock(*filter_var_a,var_a_pred,1,loader_a1,5);
	  filter_a_t = new boost::thread(&Filter_LabelBlock::run_seq, filter_a);

	  auto *load_a2 =      
	    new Load_BitmapBlock(READ_THREADS,var_lineitem_shipdate,var_lineitem_shipdate->nBlocks,1);
	  loader_a2_t = new boost::thread(&Load_BitmapBlock::run, load_a2);

	  auto *dot = 
	    new Dot_BitmapBlock_2(var_a_pred,DOT_THREADS,load_a2,11);

	  auto *load_be = 
	    new Load_DecimalVectorBlock(READ_THREADS,db.data_path,db.database_name,
	                                "lineitem","discount",var_lineitem_shipdate->nBlocks,2);

	  loader_be_t = new boost::thread(&Load_DecimalVectorBlock::run, load_be);  

	  auto *filter_b = 
	    new Filter_DecimalVectorBlock_2(*filter_var_b,WORK_THREADS,load_be,6);

	  auto *load_c = 
	    new Load_DecimalVectorBlock(READ_THREADS,db.data_path,db.database_name,
	                                "lineitem","quantity",
	                                  var_lineitem_shipdate->nBlocks,3);

	  auto *filter_c = 
	    new Filter_DecimalVectorBlock_2(*filter_var_d,WORK_THREADS,load_c,7);

	  auto *had_d = 
	    new Hadamard_FilteredBitVectorBlock_2(HAD_THREADS,dot,filter_b, "HAD_D",8);

	  auto *had_e = 
	    new Hadamard_FilteredBitVectorBlock_2(HAD_THREADS,had_d,filter_c, "HAD_E",9);

	  auto *load_f = 
	    new Load_DecimalVectorBlock(READ_THREADS,db.data_path,db.database_name,"lineitem","extendedprice",
	                                 var_lineitem_shipdate->nBlocks,4);

	  auto *map = new Map_2(lift_var_f_map,WORK_THREADS,load_be,load_f, "MAP",12);

	  auto *had_s = 
	    new Hadamard_DecimalVectorBlock_2(HAD_THREADS,had_e,map, "HAD_S",10);

	  auto *sum = 
	    new Sum(var_h,HAD_THREADS,had_s,13);
	    
  	#else // ############################################## ELSE ##############################################
		  auto *loader_a1 = 
		    new Load_LabelBlock(1,var_lineitem_shipdate,
		                        var_lineitem_shipdate->nBlocks);
		  loader_a1_t = new boost::thread(&Load_LabelBlock::run_seq, loader_a1);

		  auto *filter_a = 
		    new Filter_LabelBlock(*filter_var_a,var_a_pred,1,loader_a1);
		  filter_a_t = new boost::thread(&Filter_LabelBlock::run_seq, filter_a);

		  auto *load_a2 =      
		    new Load_BitmapBlock(READ_THREADS,var_lineitem_shipdate,var_lineitem_shipdate->nBlocks);
		  loader_a2_t = new boost::thread(&Load_BitmapBlock::run, load_a2);

		  auto *dot = 
		    new Dot_BitmapBlock_2(var_a_pred,DOT_THREADS,load_a2);

		  auto *load_be = 
		    new Load_DecimalVectorBlock(READ_THREADS,db.data_path,db.database_name,
		                                "lineitem","discount",var_lineitem_shipdate->nBlocks);

		  loader_be_t = new boost::thread(&Load_DecimalVectorBlock::run, load_be);  

		  auto *filter_b = 
		    new Filter_DecimalVectorBlock_2(*filter_var_b,WORK_THREADS,load_be);

		  auto *load_c = 
		    new Load_DecimalVectorBlock(READ_THREADS,db.data_path,db.database_name,
		                                "lineitem","quantity",
		                                  var_lineitem_shipdate->nBlocks);

		  auto *filter_c = 
		    new Filter_DecimalVectorBlock_2(*filter_var_d,WORK_THREADS,load_c);

		  auto *had_d = 
		    new Hadamard_FilteredBitVectorBlock_2(HAD_THREADS,dot,filter_b, "HAD_D");

		  auto *had_e = 
		    new Hadamard_FilteredBitVectorBlock_2(HAD_THREADS,had_d,filter_c, "HAD_E");

		  auto *load_f = 
		    new Load_DecimalVectorBlock(READ_THREADS,db.data_path,db.database_name,"lineitem","extendedprice",
		                                 var_lineitem_shipdate->nBlocks);

		  auto *map = new Map_2(lift_var_f_map,WORK_THREADS,load_be,load_f, "MAP");

		  auto *had_s = 
		    new Hadamard_DecimalVectorBlock_2(HAD_THREADS,had_e,map, "HAD_S");

		  auto *sum = 
		    new Sum(var_h,HAD_THREADS,had_s);
	#endif

  filter_b_t = new boost::thread(&Filter_DecimalVectorBlock_2::run, filter_b);

  loader_c_t = new boost::thread(&Load_DecimalVectorBlock::run, load_c);
  filter_c_t = new boost::thread(&Filter_DecimalVectorBlock_2::run, filter_c);

  loader_f_t = new boost::thread(&Load_DecimalVectorBlock::run, load_f);
  map_t = new boost::thread(&Map_2::run, map);

  filter_a_t->join();
  dot_a_t = new boost::thread(&Dot_BitmapBlock_2::run, dot);

  had_d_t = new boost::thread(&Hadamard_FilteredBitVectorBlock_2::run, had_d);
  had_e_t = new boost::thread(&Hadamard_FilteredBitVectorBlock_2::run, had_e);
  had_s_t = new boost::thread(&Hadamard_DecimalVectorBlock_2::run, had_s);
  sum_t = new boost::thread(&Sum::run, sum);

  sum_t->join();

  delete var_a_pred;

  std::cout << (*var_h) << std::endl;

  delete var_h;

  auto end = std::chrono::high_resolution_clock::now();
  
  std::cout
    << std::chrono::duration_cast<std::chrono::nanoseconds>(end-start).count()
    << endl;

  #if defined(D_PAPI) || defined(D_PAPI_OPS)
    //stop_thread_papi(th_id);
    #ifdef D_PAPI_OPS
    	print_per_ops_omp();
    #else
    	agregate_thread_values();
    	read_all_agregate_papi();
    #endif
    PAPI_shutdown();
      //print_per_thread_omp();
  #endif

  return 0;
}