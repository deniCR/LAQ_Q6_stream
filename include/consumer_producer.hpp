#ifndef STREAM_CONSUMER_PRODUCER_H
#define STREAM_CONSUMER_PRODUCER_H

#include <iostream>
#include <vector>
#include <cstdio>
#include <ctime>
#include <tuple>
#include <boost/thread/thread.hpp>
#include <boost/lockfree/queue.hpp>
#include <boost/unordered_map.hpp>
#include <boost/atomic.hpp>
#include "data_stream_type.hpp"
#include "consumer.hpp"
#include "producer.hpp"

using namespace std;
using namespace boost;

namespace stream {

	template<typename In, typename Out>
	class Consumer_Producer: 
		public Consumer<In>,
		public Producer<Out>
	{
		typedef Data_Stream_struct<In> In_elem;
		typedef Data_Stream_struct<Out> Out_elem;
	
	public:
		//Construtor
		Consumer_Producer(int _max_thread, Consumer<In> next):
			Consumer<In>(_max_thread), Producer<Out>(next)
		{}

		Consumer_Producer(int _max_thread_consumer, int _max_thread_producer,
			Consumer<In> next): Consumer<In>(_max_thread_consumer),
			Producer<Out>(next, _max_thread_producer)
		{}

		virtual ~Consumer_Producer(){}

		// Classe Consumer_Producer
		inline Out *operation_mult(In_elem *){}

		inline void operation() override
		{
			ID id=0;
			Out *out;
			In_elem *next = NULL;
			while(Consumer<In>::pop_next(&next)){
				id = next->id;
				out = operation_mult(next);
				if(out)
					Producer<Out>::send(id,*out);
			}
			Producer<Out>::end();
		}

		using Consumer<In>::run;
	};
}
#endif