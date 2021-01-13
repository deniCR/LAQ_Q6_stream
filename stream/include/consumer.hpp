/*
 * Copyright (c) 2019 Daniel Rodrigues. All rights reserved.
 */
#ifndef STREAM_CONSUMER_H
#define STREAM_CONSUMER_H

#include <iostream>
#include <vector>
#include <cstdio>
#include <ctime>
#include <tuple>
#include <boost/thread/thread.hpp>
#include <boost/atomic.hpp>
#include "include/producer.hpp"

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
	class Consumer {
		typedef Data_Stream_struct<T> Data_stream;
		typedef channel::Channel<T> Channel;

	private:
		#ifdef D_VTUNE
			vtune_tracing::VTuneDomain *vdomain_;
		#endif
		
	public:
		//Number of threads executed by the Consumer
		long consumer_threads=1;
		
		//Consumer input channel
		Channel *in;

		// Identifier assigned to the consumer by the Producer of Channel "input"
		// It is used to identify, in the Channel class, 
		// which queue the Consumer is allowed to access/modify
		int consumer_id=0;

		//PAPI operation id
		int papi_op=-5;

		Consumer(long _n_threads, stream::Producer<T> *_prev)
			:consumer_threads(_n_threads)
		{
			in = _prev->out;
			consumer_id = _prev->add_consumer();
		}

		#if defined(D_PAPI) || defined(D_PAPI_OPS)
			Consumer(long _n_threads, stream::Producer<T> *_prev,int _papi_op)
				:consumer_threads(_n_threads)
			{
				in = _prev->out;
				consumer_id = _prev->add_consumer();
				papi_op=_papi_op;
			}
		#endif

		#ifdef D_VTUNE
			Consumer(long _n_threads, stream::Producer<T> *_prev, vtune_tracing::VTuneDomain *vdomain)
				:consumer_threads(_n_threads)
			{
				in = _prev->out;
				consumer_id = _prev->add_consumer();
				vdomain_=vdomain;
			}
		#endif

		virtual ~Consumer() 
		{
			/*
			delete in;
			delete consumer_done;
			*/
		}

		inline bool producers_Done()
		{
			return in->producersDone();
		}

		inline bool finish()
		{
			return in->finish(consumer_id);
		}

		inline bool finish_lockFree()
		{
			return in->finish_lockFree(consumer_id);
		}

		inline void reuse(Data_stream *elem)
		{
			in->reuse(elem);
		}

		long getMaxThreads(){return consumer_threads;}

		/*
		 * Function executing consumer code
		 */
		virtual void operation() {}

		void papi_counter()
		{
			#if defined(D_PAPI) || defined(D_PAPI_OPS)
				//start counters
    			int thread_id = register_start_thread(papi_op);
			#endif
    			
			operation();

			#if defined(D_PAPI) || defined(D_PAPI_OPS)
				//end counters
				papi_stop_agregate(thread_id,papi_op);
			#endif
		}

		/*
		 * Remove an element from the communication channel.
		 * Removal may fail in case there are no elements in the channel.
		 *
		 * The function returns false if:
		 *	- The channel is empty AND the producer has finished producing elements.
		 *
		 * Otherwise, the function returns true.
		 * 
		 * However, object removal is only succeeded if the next variable 
		 * is changed in the function.
		 * It is advisable to set the value of the next variable 
		 * to NULL before executing the function.
		 */
		bool pop_next(Data_stream** next){
			bool result = false;

			*next=NULL;
			if(!(in->finish(consumer_id))){
				result = true;
				*next=(Data_stream*)in->forcePop(consumer_id);
			}
			return result;
		}

		bool pop_next2(Data_stream** next){
			bool result = false;
			*next=NULL;
			if(!(in->empty(consumer_id))){
				for(int i=0;i<150;++i){
					if((*next=(Data_stream*)in->pop(consumer_id))!=NULL)
						break;
				}
				result = true;
			}
			return result;
		}

		//Multi-thread execution
		virtual void run()
		{
			std::vector<boost::thread*> consumers;
			int i=0;

			#if defined(D_PAPI) || defined(D_PAPI_OPS)
				for(i=0;i<consumer_threads;++i)
					consumers.push_back(new boost::thread(&Consumer::papi_counter, this));
			#else
				for(i=0;i<consumer_threads;++i)
					consumers.push_back(new boost::thread(&Consumer::operation, this));
			#endif


			for(auto const& consumer_thread: consumers) {
				consumer_thread->join();
				delete consumer_thread;
			}
		}

		virtual void run_seq(){
			#if defined(D_PAPI) || defined(D_PAPI_OPS)
				//start counters
    			int thread_id = register_start_thread(papi_op);
			#endif
    			
			operation();

			#if defined(D_PAPI) || defined(D_PAPI_OPS)
				//end counters
				papi_stop_agregate(thread_id,papi_op);
			#endif
		}
	};
}
#endif