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

#include "consumer_time.hpp"
#include "producer_time.hpp"

#include "../../Channel/include/channel.hpp"

using namespace std;
using namespace boost;

namespace stream {

	template<typename IN, typename OUT>
	struct Consumer_Producer: 
		public Consumer<IN>,
		public Producer<OUT>
	{
		typedef Data_Stream_struct<IN> In_elem;
		typedef Data_Stream_struct<OUT> Out_elem;
		typedef channel::Channel<OUT> Channel;

	public:
		//Construtor
		Consumer_Producer(long _max_thread, stream::Producer<IN> *prev)
			: Consumer<IN>(_max_thread,prev), Producer<OUT>()
		{}

		virtual ~Consumer_Producer(){}

		using Producer<OUT>::run_seq;
		using Producer<OUT>::end;
		//using Producer<OUT>::initArray; Deprecated

		using Consumer<IN>::producers_Done;
		using Consumer<IN>::finish;
		using Consumer<IN>::pop_next;
		using Consumer<IN>::finish_lockFree;

		virtual void operation() override {}

		void papi_counter()
		{
			operation();
		}

		void run() override {
			std::vector<boost::thread*> consumers;
			int i=0;

			for(i=0;i<Consumer<IN>::consumer_threads;++i)
				consumers.push_back(new boost::thread(&Consumer_Producer<IN,OUT>::operation, this));

			for(auto const& consumer_thread: consumers) {
				consumer_thread->join();
				delete consumer_thread;
			}

			this->end();
		}
	};
}
#endif