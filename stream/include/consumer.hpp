#ifndef STREAM_CONSUMER_H
#define STREAM_CONSUMER_H

#include <iostream>
#include <vector>
#include <cstdio>
#include <ctime>
#include <tuple>
#include <boost/thread/thread.hpp>
#include <boost/lockfree/queue.hpp>
#include <boost/atomic.hpp>
#include "include/data_stream_type.hpp"

using namespace std;
using namespace boost;

namespace stream {

	template<typename T>
	class Consumer {
		typedef Data_Stream_struct<T> Data_stream;
		typedef lockfree::queue<Data_stream*> Channel;

	private:
		//Number of threads to execute
		int consumer_threads=1;
	public:
		//Communication Channels
		Channel *in = new Channel(2048);
		//Signaling variables
		boost::atomic<bool> *consumer_done = new boost::atomic<bool>(false);

		//long wait=1500;
		//boost::atomic<int> *recived = new boost::atomic<int>(0);

		Consumer() {}
		Consumer(int _n_threads):consumer_threads(_n_threads){}

		virtual ~Consumer() 
		{
			/*
			delete in;
			delete consumer_done;
			*/
		}

		int getMaxThreads(){return consumer_threads;}

		/*
		 * Function executing consumer code
		 */
		virtual void operation() {}

		/*
		 * Remove an element from the communication channel.
		 * Removal may fail in case there are no elements in the channel.
		 *
		 * The function returns false if:
		 *	- The channel is empty,
		 *	- AND the producer has finished producing elements.
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
			int i=0;
			*next=NULL;
			if(!(in->empty()) || !(*consumer_done)){
				result = true;
				for(i=0;i<30;++i){
					if(in->pop(*next)){
						/*
						if(wait>200)
							wait-=150;
						if(next)
							recived->store((*recived)+1);
						*/
						break;
					}
					else {
						//(thread suspension when waiting for new data)
						boost::this_thread::sleep_for(boost::chrono::nanoseconds(2000));
					}
				}
			}
			else {
				//(thread suspension when waiting for new data)
				boost::this_thread::sleep_for(boost::chrono::nanoseconds(5000));
			}
			/*
			if(i!=0)
				wait=(wait+(i*1000))/2;
			*/
			return result;
		}

		bool pop_next2(Data_stream** next){
			bool result = false;
			if(!(*consumer_done) || !(in->empty())){
				for(int i=0;i<150;++i){
					if(in->pop(*next))
						break;
				}
				result = true;
			}
			return result;
		}

		//Multithread execution
		virtual void run()
		{
			std::vector<boost::thread*> consumers;
			int i=0;

			for(i=0;i<consumer_threads;++i){
				consumers.push_back(new thread(&Consumer::operation, this));
			}

			for(auto const& consumer_thread: consumers) {
				consumer_thread->join();
				delete consumer_thread;
			}

		}

		//Sequential execution
		virtual void run_seq()
		{
			auto consumer_thread = new thread(&Consumer::operation, this);

			consumer_thread->join();
		}

		virtual void run_one_time(){
			operation();
		}
	};
}
#endif