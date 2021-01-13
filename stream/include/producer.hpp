/*
 * Copyright (c) 2019 Daniel Rodrigues. All rights reserved.
 */
#ifndef STREAM_PRODUCER_H
#define STREAM_PRODUCER_H

#include <iostream>
#include <vector>
#include <cstdio>
#include <ctime>
#include <tuple>
#include <boost/thread.hpp>
#include <boost/atomic.hpp>

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
	template<typename T>
	class Producer {
		typedef Data_Stream_struct<T> Data_stream;
		typedef channel::Channel<T> Channel;

	private:
		#ifdef D_VTUNE
			vtune_tracing::VTuneDomain *vdomain_;
		#endif
		//Number of output channels
		int out_counter = 0;
		//Number of threads
		long producer_threads  = 1;
		//Total number of operations to be performed by the Producer
		long n_executions = 0;

		//Broadcast of the result obtained
		inline void send(Data_stream *e)
		{ 
			out->forcePush((Data_stream *)e);
		}

		//PAPI
		int papi_op=-10;

	public:
		Channel* out;

		Producer ()
		{
			out = new Channel();
		}

		Producer (long _max_threads) 
			:producer_threads(_max_threads)
		{	
			out = new Channel();
		}

		Producer (long _max_threads, long _n_executions)
			:producer_threads(_max_threads),
			n_executions(_n_executions)
		{
			out = new Channel();
		}

		#if defined(D_PAPI) || defined(D_PAPI_OPS)
			Producer (long _max_threads, long _n_executions, int _papi_op)
				:producer_threads(_max_threads),
				n_executions(_n_executions)
			{
				out = new Channel();
				papi_op=_papi_op;
			}
		#endif

		#ifdef D_VTUNE
			Producer (long _max_threads, long _n_executions,
					  vtune_tracing::VTuneDomain *vdomain)
				:producer_threads(_max_threads),
				n_executions(_n_executions),vdomain_(vdomain)
			{
				out = new Channel();
				vdomain_=vdomain;
			}
		#endif

		virtual ~Producer() {}

		inline void setNumExecutions(long _executions) { n_executions=_executions;}

		inline int add_consumer()
		{
			++out_counter;
			return out->add_consumer();
		}

		//Adding a new communication with a consumer
		inline int add_out()
		{
			++out_counter;
			return 	out->add_consumer();
		}

		inline void send(ID _id, T *e)
		{
			if(e) {
				auto *elem = new Data_stream(_id,e,out_counter);
				send(elem);
			}
		}

		inline void send(ID _id, Data_stream *elem){
			if(elem){
				elem->newData(_id,out_counter);
				elem->empty=false;
				out->send(elem);
			}
		}

		inline Data_stream *getSinglePush()
		{
			return out->forceGetPush();
		}

		virtual void  init(void) {}
		virtual void operation(void) {}
		virtual void operation(long) {}

		void exec(long start, long end)
		{
			#if defined(D_PAPI) || defined(D_PAPI_OPS)
				//start counters
    			int thread_id = register_start_thread(papi_op);
			#endif

			if(end>start){

				for (long i=start; i<end; ++i){
					operation(i);
				}

			#if defined(D_PAPI) || defined(D_PAPI_OPS)
				//end counters
				papi_stop_agregate(thread_id,papi_op);
			#endif
			}
		}

		//Inform consumers that the data stream is over
		inline void end()
		{
			out->end();
		}

		virtual void run()
		{
			std::vector<std::thread*> producers;
			long i=0;

			this->init();

			long med = n_executions/producer_threads;
			long rest = n_executions%producer_threads;
			long start=0,end_iter=0;

			for(i=0;i<rest;++i){
				end_iter+=(med+1);
				producers.push_back(new std::thread(&Producer::exec, this, start, end_iter));
				start+=(med+1);
			}

			for(;i<producer_threads;++i){
				end_iter+=med;
				producers.push_back(new std::thread(&Producer::exec, this, start, end_iter));
				start+=med;
			}

			for(auto const& thread: producers) {
				thread->join();
				delete thread;
			}

			this->end();
		}

		virtual void run_seq(){

			#if defined(D_PAPI) || defined(D_PAPI_OPS)
				//start counters
    			int thread_id = register_start_thread(papi_op);
			#endif
    			
			operation();

			this->end();

			#if defined(D_PAPI) || defined(D_PAPI_OPS)
				//end counters
				papi_stop_agregate(thread_id,papi_op);
			#endif
		}
	};

}

#endif