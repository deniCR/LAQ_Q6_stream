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

using namespace std;
using namespace boost;

namespace stream {
	template<typename T>
	struct Producer {
		typedef Data_Stream_struct<T> Data_stream;
		typedef channel::Channel<T> Channel;

	private:
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

		virtual ~Producer() {}

		inline void setNumExecutions(long _executions) { n_executions=_executions;}

		inline int add_consumer()
		{
			++out_counter;
			return out->add_consumer();
		}

		//Add a new communication with a consumer
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

		//inline std::vector<Data_stream *> getPush()
		//{
		//	std::vector<Data_stream *> v;
		//	for(auto const& value: out)
		//		v.push_back(value.forceGetPush());
		//	return v;
		//}

		inline Data_stream *getSinglePush()
		{
			return out->forceGetPush();
		}

		virtual void  init(void) {}
		virtual void operation(void) {}
		virtual void operation(long) {}

		void exec(long start, long end)
		{
			if(end>start){
				for (long i=start; i<end; ++i){
					operation(i);
				}
			}
		}

		//Inform consumers that the stream of data is over
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
			operation();

			this->end();
		}
	};

}

#endif