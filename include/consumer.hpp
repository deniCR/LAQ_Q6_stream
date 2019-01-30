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
		//canal de comunicação entre o produtor e o consumidor
		typedef lockfree::queue<Data_stream*> Channel;

	private:
		//Número máximo de threads ativas
		int max_threads=1;

	public:
		//Canal de comunicação 
		Channel *in = new Channel(1024);
		//Variável de sinalização
		boost::atomic<bool> *consumer_done = new boost::atomic<bool>(false);
		//Tempo de suspensão (ns)
		long wait=1500;

		//Construtor vazio
		Consumer() {}
		Consumer(int _max_thread):max_threads(_max_thread)
		{}

		virtual ~Consumer() 
		{
			/*
			delete in;
			delete consumer_done;
			*/
		}

		//A operação deve descrever a ação a tomar para o processamento de 
		// um único elemento da stream **** Ainda não implementado !!!!!
		//Operação do Consumidor
		virtual void operation() {}

		//Retorna o proximo elemento
		//Se o produtor já acabou de produzir os elementos o pop pode retornar NULL
		//quando o pop retorna false já não há mais elementos para processar.
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
		//Mecanismos de lock(suspensão da thread quando está a espera de novos dados)
		bool pop_next(Data_stream** next){
			bool result = false;
			int i=0;
			if(!(*consumer_done) || !(in->empty())){
				for(i=0;i<20;++i){
					if(in->pop(*next)){
						if(wait>200)
							wait-=150;
						break;
					}
					boost::this_thread::sleep_for(boost::chrono::nanoseconds(wait+=100));
				}
				result = true;
			}
			if(i!=0)
				wait=(wait+(i*1000))/2;
			return result;
		}
		//Execução da operação com multithread
		virtual void run()
		{
			std::vector<boost::thread*> threads;

			for(int i=0;i<max_threads;++i){
				threads.push_back(new thread(&Consumer::operation, this));
			}

			for(auto const& consumer_thread: threads) {
				consumer_thread->join();
				delete consumer_thread;
			}
		}

		//Execução da operação com apenas uma thread
		virtual void run_seq()
		{
			auto consumer_thread = new thread(&Consumer::operation, this);

			consumer_thread->join();
		}

		//Execução sequencial, sem controlo de elementos
		virtual void run_one_time(){
			operation();
		}
	};
}
#endif