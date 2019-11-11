/*
 * Copyright (c) 2019 Daniel Rodrigues. All rights reserved.
 */
#include "la_query.h"
#include "EventInterface.h"
#include "engine.hpp" //Engine library inclusion

#ifndef DB_DIR
	#define DB_DIR "../../../data/la"
#endif

#ifndef DATASET
	#define DATASET 2
#endif

using namespace std;
using namespace engine;

extern HEPEvent **events;
extern map<string, unsigned> thread_ids;

// ################### ENGINE VARIABLES ###################

bool filter_var_a(std::vector<engine::Literal> *args){ return (*args)[0]>="1994-01-01"&&(*args)[0]<"1995-01-01";}
bool filter_var_b(std::vector<engine::Decimal> *args){ return (*args)[0]>=0.05&&(*args)[0]<=0.07;}
bool filter_var_d(std::vector<engine::Decimal> *args){ return (*args)[0]<24;}
engine::Decimal lift_var_f(std::vector<engine::Decimal> *args) {return (*args)[0]*(*args)[1];}

//TABLES
// "Global" access is required as load operations are performed through these objects

string tpch = string("TPCH_") +  std::to_string(DATASET);
engine::Database DB(DB_DIR,tpch,false);
engine::Bitmap *DB_lineitem_shipdate;
engine::DecimalVector *DB_lineitem_discount;
engine::DecimalVector *DB_lineitem_quantity;
engine::DecimalVector *DB_lineitem_extendedprice;
engine::FilteredBitVector *var_a_pred;
engine::Decimal *sum_results;

// ################### ENGINE VARIABLES ###################

// The number of events is determined by the tables
la_query::la_query (unsigned ncuts, unsigned engine_events)
	: DataAnalysis(ncuts, engine_events) {
	// Set to true if you want to save events that pass all cuts
	save_events = false;
	// Initialise your class variables here
}

// Use this method to fill any histograms you may want
// This method is called after each cut
// You can fill information for a specific cut by comparing the cut_name to the cut's name
// You should declare the histograms in the q6.h file
void la_query::fillHistograms (string cut_name) {

	// Use this as an example to specify a given cut
	// if (cut_name == "cut0") {
		// fill stuff....
	// }
}

// Finalize any calculations after processing all events
// If you declared and filled histograms you should write them into a file in this method
void la_query::finalize (void) {

}

// The function executes the first cycle of query 6 (strong dependencies ...), to change later
void la_query::initialize (void) {

	// Set this variable to the cut that you wish to save the events, only 1 cut is allowed
	// The first cut starts with index 0; if not set it will save after the last cut by default
	// Events will only be saved if the binary is executed with the -o option
	// If this option is set without the variable below, only the events that pass all cuts are stored
	//cut_to_save_events = 0; // uncoment this if you want to save the events that pass cut 0
	
	//Create DB
	//DB(DB_DIR,tpch,false);

  	//Variable initialization can be distributed before performing the respective tasks
  	DB_lineitem_discount = 
  		new engine::DecimalVector(DB.data_path,DB.database_name,"lineitem","discount");
   	DB_lineitem_quantity = 
  		new engine::DecimalVector(DB.data_path,DB.database_name,"lineitem","quantity");
  	DB_lineitem_extendedprice = 
  		new engine::DecimalVector(DB.data_path,DB.database_name,"lineitem","extendedprice");

  	var_a_pred = new engine::FilteredBitVector(DB_lineitem_shipdate->nLabelBlocks);

  	// Load and Filter l_shipdate_label (Before Dot Product)
  	// The var_a_pred will be accessed by all instances of the Dot task
  	for (engine::Size i = 0; i < DB_lineitem_shipdate->nLabelBlocks; ++i) {
	 
		DB_lineitem_shipdate->loadLabelBlock(i);

		var_a_pred->blocks[i] = new engine::FilteredBitVectorBlock();

	 	filter(filter_var_a,DB_lineitem_shipdate->labels[i],var_a_pred->blocks[i]);
	 	DB_lineitem_shipdate->deleteLabelBlock(i);
  	}
}

// LOAD_3
inline bool load_discount (unsigned this_event_counter) {
    DB_lineitem_discount->loadBlock(this_event_counter,
    	l_discount_DecimalVecBlock);

	return true;
}

// LOAD_4
inline bool load_quantity (unsigned this_event_counter) {
    DB_lineitem_quantity->loadBlock(this_event_counter,
    	l_quantity_DecimalVecBlock);

	return true;
}

// LOAD_5
inline bool load_extendedprice (unsigned this_event_counter) {
    DB_lineitem_extendedprice->loadBlock(this_event_counter,
    	l_extendedprice_DecimalVecBlock);

	return true;
}

// DOT
inline bool dot_shipdate (unsigned this_event_counter) {
    engine::dot(*var_a_pred,
    			*(l_shipdate_BitmapBlock),
    			var_a_block);

    //The var_a_block->nnz can be used to filter the prop...
    // nnz==0 invalidates the event
    if(var_a_block->nnz==0){
    	return false;
    }
	return true;
}

// FILTER_2
inline bool filter_discount (unsigned this_event_counter) {
    engine::filter(filter_var_b,
					l_discount_DecimalVecBlock,
					var_b_block);

    //The block->nnz can be used to filter the prop...
    // nnz==0 invalidates the event
    if(var_b_block->nnz==0){
    	return false;
    }
    return true;
}

// HAD_1
inline bool had_shipdate_discount (unsigned this_event_counter) {
	engine::krao(*(var_a_block),
			 	  *(var_b_block),
			 	  var_c_block);

    //The block->nnz can be used to filter the prop...
    // nnz==0 invalidates the event
    if(var_c_block->nnz==0){
    	return false;
    }
    return true;
}

// FILTER_3
inline bool filter_quantity (unsigned this_event_counter) {
	engine::filter(filter_var_d,
					l_quantity_DecimalVecBlock,
					var_d_block);

    //The block->nnz can be used to filter the prop...
    // nnz==0 invalidates the event
    if(var_d_block->nnz==0){
    	return false;
    }
    return true;
}

// HAD_2
// D = had_shipdate_discount
inline bool had_D_quantity (unsigned this_event_counter) {
	engine::krao(*(var_c_block),
	 			 *(var_d_block),
	 			 var_e_block);

    //The block->nnz can be used to filter the prop...
    // nnz==0 invalidates the event
    if(var_e_block->nnz==0){
    	return false;
    }
    return true;
}

// MAP
inline bool map_discount_extendedprice (unsigned this_event_counter) {
	std::vector<DecimalVectorBlock*> aux(2);
	aux[0]=((l_extendedprice_DecimalVecBlock));
	aux[1]=((l_discount_DecimalVecBlock));

	engine::lift(lift_var_f,
				 &aux,
				 var_f_block);

    //The block->nnz can be used to filter the prop...
    // nnz==0 invalidates the event
    if(var_f_block->nnz==0){
    	return false;
    }
    return true;
}

// HAD_3
// E = had_D_quantity
inline bool had_E_map (unsigned this_event_counter) {
	engine::krao(*(var_e_block),
	 			 *(var_f_block),
	 			 var_g_block);

    //The block->nnz can be used to filter the prop...
    // nnz==0 invalidates the event
    if(var_g_block->nnz==0){
    	return false;
    }
    return true;
}

// SUM
inline bool sum (unsigned this_event_counter) {
	sum_results[this_event_counter] = engine::sum(*(var_g_block));
	
	return true;
}

// END
inline bool end (unsigned this_event_counter) {
	return true;
}

int main (int argc, char *argv[]) {

	Timer query6;
	query6.start();

	#ifdef D_MPI
		MPI_Init(&argc, &argv);
	#endif

  	DB_lineitem_shipdate = 
  		new engine::Bitmap(DB.data_path,DB.database_name,"lineitem","shipdate");


	unsigned engine_events = DB_lineitem_shipdate->nBlocks;

	sum_results = new engine::Decimal[engine_events];
	for(int i=0; i<engine_events; i++){
		sum_results[i]=0;
	}

#ifdef D_ALL_LOAD
	unsigned number_of_cuts = 9;
	la_query q6 (number_of_cuts, engine_events);

	// Load operations can be added to either the DS or the DP phase

	q6.addCut("Dot", dot_shipdate);
	q6.addCut("Filter_discount", filter_discount);
	q6.addCut("Had_shipdate_discount", had_shipdate_discount);
	q6.addCut("Filter_quantity", filter_quantity);
	q6.addCut("Had_D_quantity", had_D_quantity);
	q6.addCut("Map_discount_extendedprice", map_discount_extendedprice);
	q6.addCut("Had_E_map", had_E_map);
	q6.addCut("Sum", sum);
	q6.addCut("End", end);

	//A
	q6.addCutDependency("Dot","Had_shipdate_discount");
	//B
	q6.addCutDependency("Filter_discount","Had_shipdate_discount");
	//C
	q6.addCutDependency("Filter_quantity","Had_D_quantity");
	//D
	q6.addCutDependency("Had_shipdate_discount", "Had_D_quantity");
	//E
	q6.addCutDependency("Had_D_quantity","Had_E_map");
	//F
	q6.addCutDependency("Map_discount_extendedprice","Had_E_map");
	//G
	q6.addCutDependency("Had_E_map","Sum");

	q6.addCutDependency("Sum","End");

#else

	unsigned number_of_cuts = 12;
	la_query q6 (number_of_cuts, engine_events);

	// Load operations can be added to either the DS or the DP phase

	q6.addCut("Dot", dot_shipdate);
	q6.addCut("Load_discount", load_discount);
	q6.addCut("Filter_discount", filter_discount);
	q6.addCut("Had_shipdate_discount", had_shipdate_discount);
	q6.addCut("Load_quantity", load_quantity);
	q6.addCut("Filter_quantity", filter_quantity);
	q6.addCut("Had_D_quantity", had_D_quantity);
	q6.addCut("Load_extendedprice", load_extendedprice);
	q6.addCut("Map_discount_extendedprice", map_discount_extendedprice);
	q6.addCut("Had_E_map", had_E_map);
	q6.addCut("Sum", sum);
	q6.addCut("End", end);

	//A
	q6.addCutDependency("Dot","Had_shipdate_discount");
	//l_discount
	q6.addCutDependency("Load_discount","Filter_discount");
	////l_discount -> Map
	q6.addCutDependency("Load_discount","Map_discount_extendedprice");
	//B
	q6.addCutDependency("Filter_discount","Had_shipdate_discount");
	//l_quantity
	q6.addCutDependency("Load_quantity","Filter_quantity");
	//C
	q6.addCutDependency("Filter_quantity","Had_D_quantity");
	//D
	q6.addCutDependency("Had_shipdate_discount", "Had_D_quantity");
	//E
	q6.addCutDependency("Had_D_quantity","Had_E_map");
	//l_extendedprice -> Map
	q6.addCutDependency("Load_extendedprice","Map_discount_extendedprice");
	//F
	q6.addCutDependency("Map_discount_extendedprice","Had_E_map");
	//G
	q6.addCutDependency("Had_E_map","Sum");

	q6.addCutDependency("Sum","End");

#endif

	q6.run();

	#ifdef D_MPI
		MPI_Finalize();
	#endif

	// Sum operation requires a reduction operation at the end of execution
	for(int i=1; i<engine_events; i++){
		//cout << sum_results[i] << endl;
		sum_results[0]+=sum_results[i];
	}

	delete var_a_pred;

	cout << sum_results[0] << endl;

	delete sum_results;

	cout << query6.stop()*1000 << endl;

	return 0;
}
