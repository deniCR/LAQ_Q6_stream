/*
 * Copyright (c) 2019 Daniel Rodrigues. All rights reserved.
 */
#ifndef STREAM_JOIN_H
#define STREAM_JOIN_H

#include <iostream>
#include <vector>
#include <cstdio>
#include <ctime>
#include <tuple>
#include <boost/thread/mutex.hpp>
#include <boost/thread/thread.hpp>
#include <boost/unordered_map.hpp>
#include <boost/atomic.hpp>

#include "include/consumer_time.hpp"
#include "include/producer_time.hpp"
#include "include/consumer_producer_time.hpp"
#include "../../Channel/include/channel.hpp"

#ifdef D_VTUNE
  #include "../../Vtune_ITT/tracing.h"
  using namespace vtune_tracing;
#endif

#if defined(D_PAPI) || defined(D_PAPI_OPS)
	#include "../../papi_counters/papi_stream.h"
#endif

using namespace std;
using namespace boost;

namespace stream {
	/* 
	 * The join class allows the MPSC communication type.
	 *
	 * 	Ex: input1: <id_1,Input_1> input2: <id_2,Input_2> ==> (id_1=id_2=id) =>
	 * 		==> output: <id,Output>
	 *
	 * Inputs are organized by their origin (input1 and input2) and by their id.
	 */
	template<typename IN1, typename IN2, typename OUT>
	class Join: 
		public Consumer_Producer<IN1,OUT>
	{
		typedef Data_Stream_struct<IN1> Elem1;
		typedef Data_Stream_struct<IN2> Elem2;
		typedef channel::Channel<IN2> Channel;
		typedef channel::Channel<OUT> Channel_out;

	private:
		string name = "Join";//Used for debugging
		#ifdef D_VTUNE
			vtune_tracing::VTuneDomain *vdomain_;
		#endif

		boost::mutex mutex_map1,mutex_map2;
		//Unsorted_MAP isn't thread safe ...
  		boost::unordered_map<ID,Elem1*> map1;//Data received without a pair (IN1)
  		boost::unordered_map<ID,Elem2*> map2;//Data received without a pair (IN2)

	public:
		//Additional communication channel
		Channel *in_join;
		int join_consumer_id = 0;

		Join(long _max_thread, Producer<IN1> *prev_1, Producer<IN2> *prev_2)
			: Consumer_Producer<IN1,OUT>(_max_thread,prev_1)
		{
			in_join=prev_2->out;
			join_consumer_id=prev_2->add_consumer();
		}

		Join(long _max_thread, Producer<IN1> *prev_1, Producer<IN2> *prev_2, string _name)
			: Consumer_Producer<IN1,OUT>(_max_thread, prev_1), name(_name)
		{
			in_join=prev_2->out;
			join_consumer_id=prev_2->add_consumer();
		}

		#if defined(D_PAPI) || defined(D_PAPI_OPS)
			Join(long _max_thread, Producer<IN1> *prev_1, Producer<IN2> *prev_2, string _name, int papi_op)
				: Consumer_Producer<IN1,OUT>(_max_thread,papi_op, prev_1), name(_name)
			{
				in_join=prev_2->out;
				join_consumer_id=prev_2->add_consumer();
			}
		#endif

		#ifdef D_VTUNE
			Join(long _max_thread, Producer<IN1> *prev_1, Producer<IN2> *prev_2, string _name,
				 vtune_tracing::VTuneDomain *vdomain)
				: Consumer_Producer<IN1,OUT>(_max_thread, prev_1), name(_name)
			{
				in_join=prev_2->out;
				join_consumer_id=prev_2->add_consumer();
				vdomain_=vdomain;
			}
		#endif

		virtual ~Join(){}

		//Search and execute
		void search_map1(Elem2 *value){
			ID id = 0;
			typename boost::unordered_map<ID,Elem1*>::const_iterator got;

			if(value!=NULL){
				id = value->id;

				mutex_map1.lock();
		  			got = map1.find(id);

		  		if(!(got==map1.end())){
		  			mutex_map1.unlock();
		  			exec(got->second, value);	  		
			  	}
		  		else {
		  				map2.emplace(id,value);
		  			mutex_map1.unlock();
		  		}
		  	}
		  	value=NULL;
		}

		//Search and execute
		void search_map2(Elem1 *value){
			ID id = 0;
			typename boost::unordered_map<ID,Elem2*>::const_iterator got;

			if(value!=NULL){
				id = value->id;

			  	mutex_map1.lock();
		  			got = map2.find(id);

		  		if(!(got==map2.end())){
		  			mutex_map1.unlock();
		  			exec(value, got->second);

		  		}
		  		else {
		  				map1.emplace(id,value);
		  			mutex_map1.unlock();
		  		}
		  	}
		  	value=NULL;
		}

		inline void reuse(Elem2 *elem)
		{
			in_join->reuse(elem);
		}

		// Function executed on the two elements of the stream		
		virtual void exec(Elem1 *, Elem2 *){}

		using Consumer_Producer<IN1,OUT>::producers_Done;
		using Consumer_Producer<IN1,OUT>::finish;
		using Consumer_Producer<IN1,OUT>::pop_next;
		using Consumer_Producer<IN1,OUT>::finish_lockFree;
		//using Consumer_Producer<IN1,OUT>::initArray;
		using Consumer<IN1>::reuse;

		//Input data join function
		void operation(void) override
		{
			Elem1 *value1=NULL;
			Elem2 *value2=NULL;

			#ifdef D_VTUNE
				vdomain_->set_name();
			#endif

			do {
				while(!producers_Done() || !in_join->producersDone())
				{
					if(!producers_Done() && pop_next(&value1))
					{
						search_map2(value1);
					}

					if(!in_join->producersDone() && 
						(value2=(Elem2 *)in_join->forcePop(join_consumer_id)))
					{
						search_map1(value2);
					}
				}

				while(!in_join->finish_lockFree(join_consumer_id)){
					if ((value2=(Elem2 *)in_join->forcePop(join_consumer_id))){
						search_map1(value2);
					}				
				}
				
				while(!(finish_lockFree())){
					if (pop_next(&value1)){
						search_map2(value1);
					}
				}

			} while((!(producers_Done())) || (!(finish()) ||
					(!in_join->finish(join_consumer_id))));
		}
	};

}

#endif