#ifndef STREAM_PRODUCER_H
#define STREAM_PRODUCER_H

#include <iostream>
#include <vector>
#include <cstdio>
#include <ctime>
#include <tuple>
#include <boost/thread.hpp>
#include <boost/lockfree/queue.hpp>
#include <boost/atomic.hpp>
#include "include/consumer.hpp"
#include "include/data_stream_type.hpp"

using namespace std;
using namespace boost;

namespace stream {
	template<typename T>
	class Producer {
		typedef Data_Stream_struct<T> Data_stream;
		typedef lockfree::queue<Data_stream*> Channel; 	//Lockfree queue MPMC

	private:
		boost::atomic<ID> id_counter;
		long out_counter = 0;
		int max_threads  = 1;

	public:
		std::vector<Channel*> out; 						//Consumer Communication Channels
		std::vector<boost::atomic<bool>*> done; 		//Consumer signaling variables

		//Used for debugging issues
		//boost::atomic<int> *send_stream = new boost::atomic<int>(0);

		Producer() {}

		Producer (stream::Consumer<T> c):id_counter(0),out_counter(1)
		{
			out.push_back(c.in);
			done.push_back(c.consumer_done);
		}

		Producer (int _max_threads, stream::Consumer<T> c)
			:id_counter(0),out_counter(1),max_threads(_max_threads)
		{
			out.push_back(c.in);
			done.push_back(c.consumer_done);
		}

		Producer (Channel* const &_out, boost::atomic<bool> * &_done) 
			:id_counter(0),out_counter(1)
		{	
			out.push_back(_out);
			done.push_back(_done);
		}

		virtual ~Producer() {}

		//Adding a new communication with a consumer
		void add_consumer(stream::Consumer<T> c)
		{
			out.push_back(c.in);
			++out_counter;
			done.push_back(c.consumer_done);
		}

		//Adding a new communication with a consumer
		void add_out(Channel* const &_out, boost::atomic<bool> * &_done)
		{
			out.push_back(_out);
			++out_counter;
			done.push_back(_done);
		}

		//Broadcast of the result obtained
		void send(Data_stream *e)
		{ 
			for(auto const& value: out) {
				while(!value->push(e));
			}
			//send_stream->store(*send_stream+1);
		}

		void send(T e)
		{
			auto *elem = new Data_stream(id_counter++,e,out_counter);
			send(elem);
		}

		void send(ID _id, T e)
		{
			auto *elem = new Data_stream(_id,e,out_counter);
			send(elem);
		}

		/*
		 * Performing the production operation of stream elements.
		 *
		 * The code should make explicit the creation and sending of the elements.
		 *
		 */
		virtual void operation(void) {}

		//Inform consumers that the data stream is over
		void end()
		{
			/*
			unsigned long size = done.size();
			for(unsigned long i=0;i<size;++i){
				if(done[i]){
					done[i]->store(true);
					done[i]=NULL;
				}
			}
			*/
			for(auto &value: done){
				(value)->store(true);
			}
		}

		virtual void run(){
			thread *producer_thread=NULL;
			producer_thread = new thread(&Producer::operation, this);
			producer_thread->join();
			end();
		}

		virtual void run_seq(){
			operation();
			end();
		}
	};

}

#endif