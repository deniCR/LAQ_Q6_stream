#ifndef STREAM_JOIN_H
#define STREAM_JOIN_H

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
	 * A classe Join pretende agregar dois inputs para um output
	 * Ex: input1: <id_1,Block_1> input2: <id_2,Block_2> 
	 * 			==(id_1=id_2=id)=> output: <id,<Block_1,Block_2>>
	 * Os inputs são organizados pelos ids, mantendo um certa "ordem".
	 */
	template<typename P1, typename P2>
	class Join: 
		public Producer<std::tuple<P1,P2>*>,
		public Consumer<P1>
	{
		typedef Data_Stream_struct<P1> Elem1;
		typedef Data_Stream_struct<P2> Elem2;
		typedef std::tuple<P1,P2> Join_data;			//Elementos do Input1 e Input2
		typedef Data_Stream_struct<Join_data*> Data_stream;
		typedef lockfree::queue<Elem2*> Channel;		//channel adicional
	
	private:
  		unordered_map<ID,P1> map1;				//Map dos Inputs de 1
  		unordered_map<ID,P2> map2;				//Map dos Inputs de 2

	public:
		//channel adicional
		Channel *in_join = new Channel(1024);
		//Variável de sinalização adicional
		boost::atomic<bool> *done_join = new boost::atomic<bool>(false);

		//Construtor
		Join(int _max_thread, Consumer<Join_data*> next):
		Producer<Join_data*>(next), Consumer<P1>(_max_thread){}

		virtual ~Join(){}

		//Search and send
		void search_map1(Elem2 *value){
			ID id = 0;
			if(value!=NULL){
				id = value->id;
		  		typename unordered_map<ID,P1>::const_iterator got = map1.find (id);
		  		if(!(got==map1.end())){
		  			Join_data *tt = new Join_data(got->second,value->data);
		  			Data_stream *t = new Data_stream(id,tt);
		  			this->send(t);
		  		}
		  		else {
		  			map2.emplace(id,value->data);
		  		}
		  	}
		  	value=NULL;
		}
		//Search and send
		void search_map2(Elem1 *value){
			ID id = 0;
			if(value!=NULL){
				id = value->id;
		  		typename unordered_map<ID,P2>::const_iterator got = map2.find (id);
		  		if(!(got==map2.end())){
		  			Join_data *tt = new Join_data(value->data,got->second);
		  			Data_stream *t = new Data_stream(id,tt);
		  			this->send(t);
		  		}
		  		else {
		  			map1.emplace(id,value->data);
		  		}
		  	}
		  	value=NULL;
		}
		
		using Producer<std::tuple<P1,P2>*>::operation;
		using Producer<std::tuple<P1,P2>*>::run;
		using Producer<std::tuple<P1,P2>*>::run_seq;

		//Função de join
		inline void operation(void) override
		{
			Elem1 *value1=NULL;
			Elem2 *value2=NULL;

			while(!(*Consumer<P1>::consumer_done) || !(*done_join))
			{
				value1=NULL;
				if(Consumer<P1>::pop_next(&value1))
					search_map2(value1);
				value2=NULL;
				if(in_join->pop(value2))
					search_map1(value2);
			}

			while(!in_join->empty()){
				if (in_join->pop(value2)){
					search_map1(value2);
				}
			}
			
			while(!(Consumer<P1>::in->empty())){
				if (Consumer<P1>::pop_next(&value1)){
					search_map2(value1);
				}
			}
		}
	};

}

#endif