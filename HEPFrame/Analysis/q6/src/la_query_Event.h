/*
 * Copyright (c) 2019 Daniel Rodrigues. All rights reserved.
 */
#ifndef la_query_Event_h
#define la_query_Event_h

#include "engine.hpp" //Engine library inclusion
#include <iostream>

//Check the size of each event to ensure a relatively large number of events (~ 100 +)
//With BSIZE=32Ki blocks la_query events consume ~3MB
#ifndef MAX_MEM_CAPACITY
  #define MAX_MEM_CAPACITY 1500 //MB
#endif

#define MIN_NUM_EVENTS 50

using namespace std;

class HEPEvent {
	//TTree *fChain; Not required for query execution

  public:
    long id;
    bool *_done;

    // Blocks used in the query pipeline
    // Operations are always performed on blocks with the same identifier (same "event")

    engine::BitmapBlock *l_shipdate_BitmapBlock;
    engine::DecimalVectorBlock *l_discount_DecimalVecBlock;
    engine::DecimalVectorBlock *l_quantity_DecimalVecBlock;
    engine::DecimalVectorBlock *l_extendedprice_DecimalVecBlock;
    engine::FilteredBitVectorBlock *var_a_block;
    engine::FilteredBitVectorBlock *var_b_block;
    engine::FilteredBitVectorBlock *var_c_block;
    engine::FilteredBitVectorBlock *var_d_block;
    engine::FilteredBitVectorBlock *var_e_block;
    engine::DecimalVectorBlock *var_f_block;
    engine::FilteredDecimalVectorBlock *var_g_block;
    engine::Decimal var_h;

  	HEPEvent (void);

  	~HEPEvent (void);

    // Initialization of event variables
  	bool init (void);

    // Load of tables
  	int loadEvent (long entry);

    inline bool done(){
      return (*_done);
    }

    inline void set_done(bool _done_){
      (*_done)=_done_;
    }

    static long event_size();

    static unsigned max_num_events;

    static void init_memory_controler();

    static long getMaxNumEvents();
};
#endif

