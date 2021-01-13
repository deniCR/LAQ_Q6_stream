#ifndef CHANNEL_INCLUDE_H
#define CHANNEL_INCLUDE_H

#include <queue>
#include <mutex> 
#include <iostream>
#include <chrono>
#include <thread>
#include <boost/atomic.hpp>
#include <boost/thread/thread.hpp>
#include <boost/chrono.hpp>
#include <boost/fiber/condition_variable.hpp>
#include "data_stream_type.hpp"
#include <stdlib.h>
#include <assert.h>

using namespace stream;

namespace channel {

	template<typename T>
	class Channel {
		typedef Data_Stream_struct<T> Data_stream;

	private:
		/* ##### CONSUMER ##### */
		//Counters of the number of elements processed by the Consumers
		long consumer_count[2]={0,0};
		std::mutex consumer_m[2];
		boost::mutex cons_wait[2];
		boost::condition_variable_any cons_var[2];

		/* ##### PRODUCER ##### */
		//Counter for the number of elements produced
		long producer_count=0;
		long safe_distance = 6;
		boost::atomic<bool> producers_done;
		std::mutex producer_m;

		int consumers_n=0;

	public:
		std::queue<Data_stream*> *freeBlocks;
		std::vector<std::queue<Data_stream*>*> consumerBlocks;

		Channel()
			:producers_done(false),freeBlocks(),consumerBlocks(2,NULL)
		{
			freeBlocks = new std::queue<Data_stream*>();
			consumerBlocks[0] = new std::queue<Data_stream*>();
		}

		~Channel()
		{
			//free(array);
		}

		void deleteBlocks(){
			producer_m.lock();
			while(!freeBlocks->empty()){
				delete freeBlocks->back();
				freeBlocks->pop();
			}
			producer_m.unlock();
		}

		/* Deprecated
		inline void initArray()
		{
			int init_size = init_size_channel();
			Data_stream *init_data = new Data_stream[init_size];
			while(!producer_m.try_lock());
			for(int i=0; i<init_size; ++i){
				freeBlocks->push(&init_data[i]);
			}
			producer_m.unlock();
		}
		*/

		inline void initArray(int size)
		{
			Data_stream *init_data = new Data_stream[size];
			for(int i=0; i<size; ++i){
				freeBlocks->push(&init_data[i]);
			}
		}

		inline void fillArray(int size)
		{
			Data_stream *init_data = new Data_stream[size];
			while(!producer_m.try_lock());
			for(int i=0; i<size; ++i){
				freeBlocks->push(&init_data[i]);
			}
			producer_m.unlock();
		}

		inline int add_consumer()
		{
			int id = consumers_n;
			consumers_n++;

			consumerBlocks[1] = new std::queue<Data_stream*>();

			return id;
		}

		inline void end()
		{
			producers_done.store(true,boost::memory_order_seq_cst);
			cons_var[0].notify_all();
			cons_var[1].notify_all();
			//deleteBlocks();
		}

		inline bool empty(int c_id)
		{
			bool r=false;
			//consumer_m[c_id].lock();
			//producer_m.lock();
			while(try_lock(consumer_m[c_id],producer_m)!=-1);
			r = (producer_count==consumer_count[c_id]);
			producer_m.unlock();
			consumer_m[c_id].unlock();
			return	r;
		}
	
		/* Deprecated
		inline bool empty_lockFree()
		{
			bool r=false;
			long count = consumer_count_atomic.load();
			producer_m.lock();
				r = (count==producer_count);
			producer_m.unlock();
			return	r;
		}
		*/

		inline bool empty_lockFree(int c_id)
		{
			return (producer_count<=consumer_count[c_id]);
		}
	
		inline bool finish(int c_id)
		{
			bool result = producers_done.load(boost::memory_order_seq_cst);

			if(result){
				result = empty(c_id);
			}
			return result;
		}
	
		/* Deprecated
		inline bool finish_lockFree()
		{
			bool r = producers_done.load(boost::memory_order_seq_cst);
			if(r){
				r = empty_lockFree();
			}
			return r;
		}
		*/

		inline bool finish_lockFree(int c_id)
		{
			return (empty_lockFree(c_id) && producers_done.load(boost::memory_order_seq_cst));
		}

		inline bool producersDone()
		{
			return producers_done.load(boost::memory_order_seq_cst);
		}

		/* Deprecated
		Data_stream *pop(int c_id)
		{
			long index=0;
			Data_stream *result=NULL;
	
			//Lock para garantir a correta leitura do valor e 
			//para efetuar a alteração do mesmo

			consumer_m[c_id].lock(); //LOCK
			index=consumer_index[c_id];
			result=array[index];
	
			if(consumer_count[c_id]<producer_count && (result && !(result->empty)))
			{
				++consumer_index[c_id];++consumer_count[c_id];

				//std::cout << "Pop element " << index << std::endl; 
	
				if(consumer_index[c_id]>=size)
					consumer_index[c_id]=(consumer_count[c_id]%size);
	
				consumer_m[c_id].unlock();
			}
			else 
			{
				consumer_m[c_id].unlock();
				result=NULL;
			}
	
			return	result;
		}
		*/

		inline Data_stream *pop(int c_id)
		{
			Data_stream *result=NULL;
	
			consumer_m[c_id].lock();

			if(!consumerBlocks[c_id]->empty()){
				++consumer_count[c_id];
				//Can be more efficient ...
				result = consumerBlocks[c_id]->front();
				//remove the element from the queue ...
				consumerBlocks[c_id]->pop();
			}

			consumer_m[c_id].unlock();
	
			return	result;
		}

		inline Data_stream *forcePop(int c_id)
		{
			Data_stream *elem=NULL;
			for(int i=0; i<5 && ((elem=pop(c_id))==NULL); ++i){
				if(!producers_done.load(boost::memory_order_seq_cst) 
					&& ((producer_count-consumer_count[c_id])<safe_distance))
				{
					boost::unique_lock<boost::mutex> lock{cons_wait[c_id]};
					auto wake_up = boost::chrono::steady_clock::now() + boost::chrono::nanoseconds(30000);
					cons_var[c_id].wait_until(cons_wait[c_id], wake_up);
					//cons_var[c_id].wait(cons_wait[c_id]);
				}
				else
					cons_var[c_id].notify_all();
			}
			return elem;
		}
	
		inline bool forcePop(Data_stream *elem, int c_id)
		{
			bool result=false;
			for(int i=0;((elem=pop(c_id))==NULL) && i<5; ++i){
				if(!producers_done.load(boost::memory_order_seq_cst)
					&& ((producer_count-consumer_count[c_id])<safe_distance))
				{
					boost::unique_lock<boost::mutex> lock{cons_wait[c_id]};
					auto wake_up = boost::chrono::steady_clock::now() + boost::chrono::nanoseconds(30000);
					cons_var[c_id].wait_until(cons_wait[c_id], wake_up);
					//cons_var[c_id].wait(cons_wait[c_id]);
				}
				else
					cons_var[c_id].notify_all();
			}
	
			if(elem!=NULL && !elem->empty)
				result=true;
			return result;
		}

		inline void send(Data_stream *elem)
		{
			consumer_m[0].lock();
			consumerBlocks[0]->push(elem);
			consumer_m[0].unlock();
			cons_var[0].notify_one();

			if(consumers_n>1){
				consumer_m[1].lock();
				consumerBlocks[1]->push(elem);
				consumer_m[1].unlock();
				cons_var[1].notify_one();
			}
		}

		inline void reuse(Data_stream *elem)
		{
			if(elem->clear()) {
				if(!producersDone())
				{
					producer_m.lock();
					freeBlocks->push(elem);
					producer_m.unlock();
				}
				else 
				{
					elem->remove();
				}
			}
		}

		/* Deprecated
		bool push(Data_stream *elem)
		{
			bool r=true;
			long index=0;

			bool aux_1=false,aux_2=false;
	
			producer_m.lock();
			index=producer_index;
	
			if(producer_index<size && array[index]==NULL)
			{
				array[index]=elem;
	
				++producer_index;++producer_count;
	
				if(producer_index==size)
					producer_index=(producer_count%size);
	
				producer_m.unlock();
			}
			else
			{
				aux_1 = (producer_count-consumer_count[0])>=(((size)/5));
				if(consumers_n==2)
					aux_2 = (producer_count-consumer_count[1])>=(((size)/5));

				if(producer_index==0 && (aux_1 || aux_2))
				{
					resize();
				}
				producer_m.unlock();
				r=false;
			}
	
			return	r;
		}
		*/

		void push(Data_stream *elem)
		{
			producer_m.lock();
			++producer_count;

			if(!freeBlocks->empty()){
				elem = freeBlocks->front();
				freeBlocks->pop();
				std::cout << freeBlocks->size() << std::endl;
				producer_m.unlock();
			}
			else{
				producer_m.unlock();
				elem = new Data_stream();
			}
		}

		/* Deprecated
		Data_stream *getPush()
		{
			Data_stream *r=NULL;
			long index=0;

			bool aux_1=false,aux_2=false;
	
			producer_m.lock();
			index=producer_index;

			if(array[index]==NULL && index<size)
				array[index]=new Data_stream();

			if(index<size && (array[index]->empty))
			{
				r=array[index];
	
				++producer_index;++producer_count;
	
				if(producer_index==size)
					producer_index=(producer_count%size);
	
				producer_m.unlock();
			}
			else
			{
				aux_1 = (producer_count-consumer_count[0])>=(((size)/5));
				if(consumers_n==2)
					aux_2 = (producer_count-consumer_count[1])>=(((size)/5));

				if(producer_index==0 && (aux_1 || aux_2))
				{
					resize();
				}
				producer_m.unlock();
			}
	
			return	r;
		}
		*/
	
		inline Data_stream *getPush()
		{
			Data_stream *result=NULL;

			producer_m.lock();
			++producer_count;

			if(!freeBlocks->empty()){
				result = freeBlocks->front();
				freeBlocks->pop();
				producer_m.unlock();
			}
			else{
				//initArray(16);
				producer_m.unlock();
				result = new Data_stream();
			}
	
			return	result;
		}

		inline void forcePush(Data_stream *elem)
		{
			push(elem);
		}

		inline Data_stream *forceGetPush()
		{
			Data_stream *result=NULL;
			result=getPush();
			return result;
		}
	};
}
#endif