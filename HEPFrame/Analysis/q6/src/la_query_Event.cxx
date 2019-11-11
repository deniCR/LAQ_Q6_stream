/*
 * Copyright (c) 2019 Daniel Rodrigues. All rights reserved.
 */
#include "la_query_Event.h"
#include "la_query_EventBranches.h"
#include "engine.hpp"

extern bool tree_filled;
extern engine::Bitmap *DB_lineitem_shipdate;
extern engine::DecimalVector *DB_lineitem_discount;
extern engine::DecimalVector *DB_lineitem_quantity;
extern engine::DecimalVector *DB_lineitem_extendedprice;
extern HEPEvent *events;

HEPEvent::HEPEvent (void) {
	var_h = 0;

    _done = new bool(true);

	l_shipdate_BitmapBlock = NULL;
  	l_discount_DecimalVecBlock = NULL;
  	l_quantity_DecimalVecBlock = NULL;
  	l_extendedprice_DecimalVecBlock = NULL;

	var_a_block = NULL;
	var_b_block = NULL;
	var_c_block = NULL;
	var_d_block = NULL;
	var_e_block = NULL;
	var_f_block = NULL;
	var_g_block = NULL;
}

HEPEvent::~HEPEvent (void) {
    if(_done!=NULL){
        delete _done;
        _done = NULL;
    }

	if(l_shipdate_BitmapBlock!=NULL){
		delete l_shipdate_BitmapBlock;
		delete l_discount_DecimalVecBlock;
		delete l_quantity_DecimalVecBlock;
		delete l_extendedprice_DecimalVecBlock;
		delete var_a_block;
		delete var_b_block;
		delete var_c_block;
		delete var_d_block;
		delete var_e_block;
		delete var_f_block;
		delete var_g_block;

		l_shipdate_BitmapBlock=NULL;
		l_discount_DecimalVecBlock=NULL;
		l_quantity_DecimalVecBlock=NULL;
		l_extendedprice_DecimalVecBlock=NULL;
		var_a_block=NULL;
		var_b_block=NULL;
		var_c_block=NULL;
		var_d_block=NULL;
		var_e_block=NULL;
		var_f_block=NULL;
		var_g_block=NULL;
	}
}

bool HEPEvent::init (void) {

    (*_done)=true;

	l_shipdate_BitmapBlock = new engine::BitmapBlock();
  	l_discount_DecimalVecBlock = new engine::DecimalVectorBlock();
  	l_quantity_DecimalVecBlock = new engine::DecimalVectorBlock();
  	l_extendedprice_DecimalVecBlock = new engine::DecimalVectorBlock();

	var_a_block = new engine::FilteredBitVectorBlock();
	var_b_block = new engine::FilteredBitVectorBlock();
	var_c_block = new engine::FilteredBitVectorBlock();
	var_d_block = new engine::FilteredBitVectorBlock();
	var_e_block = new engine::FilteredBitVectorBlock();
	var_f_block = new engine::DecimalVectorBlock();
	var_g_block = new engine::FilteredDecimalVectorBlock();

	return true;
}

int HEPEvent::loadEvent (long entry) {
	int _id = entry;
	id = _id;

	//ONE LOAD
	DB_lineitem_shipdate->loadBlock(_id,l_shipdate_BitmapBlock);

    #ifdef D_ALL_LOAD
	   //ALL LOAD
	   DB_lineitem_discount->loadBlock(_id,l_discount_DecimalVecBlock);
	   DB_lineitem_quantity->loadBlock(_id,l_quantity_DecimalVecBlock);
	   DB_lineitem_extendedprice->loadBlock(_id,l_extendedprice_DecimalVecBlock);
    #endif

    (*_done)=false;

	return 1;
}

unsigned HEPEvent::max_num_events=100;

void HEPEvent::init_memory_controler()
{
    HEPEvent::max_num_events = ((0.8*MAX_MEM_CAPACITY)*1024*1024)/HEPEvent::event_size();

    if(max_num_events<MIN_NUM_EVENTS){

        std::cout << "Check event size and MAX_MEM_CAPACITY." << endl;
        std::cout << "Maximum number of events is less than the recommended value." << endl;
        std::cout << "Actual value: " << max_num_events << " Recommended value: " << MIN_NUM_EVENTS << endl;
        std::terminate();
    }
}

long HEPEvent::getMaxNumEvents(){
    return HEPEvent::max_num_events;
  }

long HEPEvent::event_size()
{
    long size=0;

    engine::BitmapBlock *l_shipdate_BitmapBlock_2;
    engine::DecimalVectorBlock *l_discount_DecimalVecBlock_2;
    engine::DecimalVectorBlock *l_quantity_DecimalVecBlock_2;
    engine::DecimalVectorBlock *l_extendedprice_DecimalVecBlock_2;
    engine::FilteredBitVectorBlock *var_a_block_2;
    engine::FilteredBitVectorBlock *var_b_block_2;
    engine::FilteredBitVectorBlock *var_c_block_2;
    engine::FilteredBitVectorBlock *var_d_block_2;
    engine::FilteredBitVectorBlock *var_e_block_2;
    engine::DecimalVectorBlock *var_f_block_2;
    engine::FilteredDecimalVectorBlock *var_g_block_2;
    engine::Decimal var_h_2;

    l_shipdate_BitmapBlock_2 = new engine::BitmapBlock();
    l_discount_DecimalVecBlock_2 = new engine::DecimalVectorBlock();
    l_quantity_DecimalVecBlock_2 = new engine::DecimalVectorBlock();
    l_extendedprice_DecimalVecBlock_2 = new engine::DecimalVectorBlock();

    var_a_block_2 = new engine::FilteredBitVectorBlock();
    var_b_block_2 = new engine::FilteredBitVectorBlock();
    var_c_block_2 = new engine::FilteredBitVectorBlock();
    var_d_block_2 = new engine::FilteredBitVectorBlock();
    var_e_block_2 = new engine::FilteredBitVectorBlock();
    var_f_block_2 = new engine::DecimalVectorBlock();
    var_g_block_2 = new engine::FilteredDecimalVectorBlock();

    size+=l_shipdate_BitmapBlock_2->bytes();
    size+=l_discount_DecimalVecBlock_2->bytes();
    size+=l_quantity_DecimalVecBlock_2->bytes();
    size+=l_extendedprice_DecimalVecBlock_2->bytes();

    size+=var_a_block_2->bytes();
    size+=var_b_block_2->bytes();
    size+=var_c_block_2->bytes();
    size+=var_d_block_2->bytes();
    size+=var_e_block_2->bytes();
    size+=var_f_block_2->bytes();
    size+=var_g_block_2->bytes();

    delete(l_shipdate_BitmapBlock_2);
    delete(l_discount_DecimalVecBlock_2);
    delete(l_quantity_DecimalVecBlock_2);
    delete(l_extendedprice_DecimalVecBlock_2);

    delete(var_a_block_2);
    delete(var_b_block_2);
    delete(var_c_block_2);
    delete(var_d_block_2);
    delete(var_e_block_2);
    delete(var_f_block_2);
    delete(var_g_block_2);

    return size;
    // ~ 100xBSIZE Bytes ~ 100xBSIZE/1024/1024 MiB
}