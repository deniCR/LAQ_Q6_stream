/*
 * Copyright (c) 2019 Daniel Rodrigues. All rights reserved.
 */
#ifndef STREAM_CONSUMER_PRODUCER_H
#define STREAM_CONSUMER_PRODUCER_H

#include <iostream>
#include <vector>
#include <cstdio>
#include <ctime>
#include <tuple>
#include <boost/thread/thread.hpp>
#include <boost/atomic.hpp>
#include "consumer.hpp"
#include "producer.hpp"
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

	template<typename IN, typename OUT>
	class Consumer_Producer: 
		public Consumer<IN>,
		public Producer<OUT>
	{
		typedef Data_Stream_struct<IN> In_elem;
		typedef Data_Stream_struct<OUT> Out_elem;
		typedef channel::Channel<OUT> Channel;

	private:
		#ifdef D_VTUNE
			vtune_tracing::VTuneDomain *vdomain_;
		#endif
		string task;

		//PAPI
		int papi_op=-1;

	public:
		//Construtor
		Consumer_Producer(long _max_thread, stream::Producer<IN> *prev)
			: Consumer<IN>(_max_thread,prev), Producer<OUT>()
		{}

		#if defined(D_PAPI) || defined(D_PAPI_OPS)
			Consumer_Producer(long _max_thread, int _papi_op, stream::Producer<IN> *prev)
				: Consumer<IN>(_max_thread,prev),Producer<OUT>(_max_thread)
			{
				papi_op=_papi_op;
			}

			Consumer_Producer(long _max_thread, stream::Producer<IN> *prev,long _n_executions)
				: Consumer<IN>(_max_thread,prev),Producer<OUT>(_max_thread, _n_executions)
			{}

			Consumer_Producer(long _max_thread, int papi_op, stream::Producer<IN> *prev,long _n_executions)
				: Consumer<IN>(_max_thread,prev),Producer<OUT>(_max_thread, _n_executions)
			{
				papi_op=_papi_op;
			}
		#endif

		#ifdef D_VTUNE
			Consumer_Producer(long _max_thread, stream::Producer<IN> *prev, 
								vtune_tracing::VTuneDomain *vdomain)
				: Consumer<IN>(_max_thread,prev), Producer<OUT>()
			{
				vdomain_=vdomain;
			}

			Consumer_Producer(long _max_thread, stream::Producer<IN> *prev,long _n_executions,
								vtune_tracing::VTuneDomain *vdomain)
				: Consumer<IN>(_max_thread,prev,vdomain),Producer<OUT>(_max_thread, _n_executions)
			{
				vdomain_=vdomain;
			}
		#endif

		virtual ~Consumer_Producer(){}

		using Producer<OUT>::run_seq;
		using Producer<OUT>::end;
		using Producer<OUT>::initArray;

		using Consumer<IN>::producers_Done;
		using Consumer<IN>::finish;
		using Consumer<IN>::pop_next;
		using Consumer<IN>::finish_lockFree;

		virtual void operation() override {}

		#if defined(D_PAPI) || defined(D_PAPI_OPS)
			void papi_counter()
			{
				//start counters
	    		int thread_id = register_start_thread(papi_op);
	    			
				operation();

				//end counters
				papi_stop_agregate(thread_id,papi_op);
			}
		#endif

		void run() override {
			std::vector<boost::thread*> consumers;
			int i=0;

			#if defined(D_PAPI) || defined(D_PAPI_OPS)
				for(i=0;i<Consumer<IN>::consumer_threads;++i)
					consumers.push_back(new boost::thread(&Consumer_Producer<IN,OUT>::papi_counter, this));
			#else
				for(i=0;i<Consumer<IN>::consumer_threads;++i)
					consumers.push_back(new boost::thread(&Consumer_Producer<IN,OUT>::operation, this));
			#endif

			for(auto const& consumer_thread: consumers) {
				consumer_thread->join();
				delete consumer_thread;
			}

			this->end();
		}
	};
}
#endif