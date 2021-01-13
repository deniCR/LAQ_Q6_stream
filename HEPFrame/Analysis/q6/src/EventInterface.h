// ###################################################################################
//
//							!!Disclaimer!!
//
// ###################################################################################
//
// Add variables in the EventData class, between the public and constructor statements
//
// ###################################################################################

#include <vector>
#include "la_query_Event.h"

extern HEPEvent **events;

/*
 *	 defines below
 */
#define done events[this_event_counter]->_done

#define l_shipdate_BitmapBlock events[this_event_counter]->l_shipdate_BitmapBlock
#define l_discount_DecimalVecBlock events[this_event_counter]->l_discount_DecimalVecBlock
#define l_quantity_DecimalVecBlock events[this_event_counter]->l_quantity_DecimalVecBlock
#define l_extendedprice_DecimalVecBlock events[this_event_counter]->l_extendedprice_DecimalVecBlock

#define var_a_block events[this_event_counter]->var_a_block
#define var_b_block events[this_event_counter]->var_b_block
#define var_c_block events[this_event_counter]->var_c_block
#define var_d_block events[this_event_counter]->var_d_block
#define var_e_block events[this_event_counter]->var_e_block
#define var_f_block events[this_event_counter]->var_f_block
#define var_g_block events[this_event_counter]->var_g_block
#define var_h events[this_event_counter]->var_h