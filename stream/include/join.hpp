#ifndef STREAM_NEW_JOIN_H
#define STREAM_NEW_JOIN_H

#include <iostream>
#include <vector>
#include <cstdio>
#include <ctime>
#include <tuple>
#include <boost/thread/thread.hpp>
#include <boost/lockfree/queue.hpp>
#include <boost/unordered_map.hpp>
#include <boost/atomic.hpp>
#include "include/consumer.hpp"
#include "include/producer.hpp"
#include "include/data_stream_type.hpp"

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
	class New_Join: 
		public Producer<OUT>,
		public Consumer<IN1>
	{
		typedef Data_Stream_struct<IN1> Elem1;
		typedef Data_Stream_struct<IN2> Elem2;
		typedef lockfree::queue<Elem2*> Channel;    //Additional communication channel (IN2)
	
	private:
		string name = "Join";						//Used for debugging issues

		//Unsorted_MAP isn't thread safe ...
  		unordered_map<ID,Elem1*> map1;				//Data received without a pair (IN1)
  		unordered_map<ID,Elem2*> map2;				//Data received without a pair (IN2)

	public:
		//Additional communication channel
		Channel *in_join = new Channel(2048);
		//Additional signaling variable
		boost::atomic<bool> *done_join = new boost::atomic<bool>(false);

		New_Join(int _max_thread, Consumer<OUT> next):
		Producer<OUT>(next), Consumer<IN1>(_max_thread){}

		New_Join(int _max_thread, Consumer<OUT> next, string _name):
		Producer<OUT>(next), Consumer<IN1>(_max_thread), name(_name){}

		New_Join(int _max_thread, Channel *_out, boost::atomic<bool> *_done):
		Producer<OUT>(_out, _done), Consumer<IN1>(_max_thread){}

		New_Join(int _max_thread, Channel *_out, boost::atomic<bool> *_done, string _name):
		Producer<OUT>(_out, _done), Consumer<IN1>(_max_thread), name(_name){}

		virtual ~New_Join(){}

		//Search and execute
		void search_map1(Elem2 *value){
			ID id = 0;
			typename unordered_map<ID,Elem1*>::const_iterator got;

			if(value!=NULL){
				id = value->id;
		  		got = map1.find(id);
		  		if(!(got==map1.end())){
		  			exec(got->second, value);
		  			map1.erase(id);
		  		}
		  		else {
		  			map2.emplace(id,value);
		  		}
		  	}
		  	value=NULL;
		}

		//Search and execute
		void search_map2(Elem1 *value){
			ID id = 0;
			typename unordered_map<ID,Elem2*>::const_iterator got;

			if(value!=NULL){
				id = value->id;
		  		got = map2.find(id);
		  		if(!(got==map2.end())){
		  			exec(value, got->second);
		  			map2.erase(id);
		  		}
		  		else {
		  			map1.emplace(id,value);
		  		}
		  	}
		  	value=NULL;
		}

		/*
		 * Function executed on the two elements of the stream
		 */
		virtual void exec(Elem1 *, Elem2 *){}

		using Producer<OUT>::operation;
		using Producer<OUT>::run;
		using Producer<OUT>::run_seq;

		//Input data join function
		inline void operation(void) override
		{
			Elem1 *value1=NULL;
			Elem2 *value2=NULL;

			do {
				while(!(*Consumer<IN1>::consumer_done) || !(*done_join))
				{
					value1=NULL;
					if(Consumer<IN1>::pop_next(&value1))
						search_map2(value1);
					value2=NULL;
					if(in_join->pop(value2))
						search_map1(value2);
				}

				while(!in_join->empty()){
					value2 = NULL;
					if (in_join->pop(value2)){
						search_map1(value2);
					}				}
				
				while(!(Consumer<IN1>::in->empty())){
					value1 = NULL;
					if (Consumer<IN1>::pop_next(&value1)){
						search_map2(value1);
					}
				}
			} while((!(*Consumer<IN1>::consumer_done) || !(*done_join)) || 
					(!(Consumer<IN1>::in->empty()) || (!in_join->empty())));

			this->end();
		}
	};

}

#endif