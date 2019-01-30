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
		//Lockfree queue MPMC
		typedef lockfree::queue<Data_stream*> Channel;

	private:
		boost::atomic<ID> id_counter;
		long out_counter = 0;
		int max_threads  = 1;

	public:
		std::vector< Channel *> out; 				//queues dos consumidores
		std::vector< boost::atomic<bool> *> done; 	//variáveis de sinalização ...

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

		//Adição de um output/canal de um determinado consumidor
		void add_consumer(stream::Consumer<T> c)
		{
			out.push_back(c.in);
			++out_counter;
			done.push_back(c.consumer_done);
		}

		//Adição de um output/canal
		void add_out(Channel* const &_out, boost::atomic<bool> * &_done)
		{
			out.push_back(_out);
			++out_counter;
			done.push_back(_done);
		}

		//Envio do elemento para os consumidores
		void send(Data_stream *e)
		{ 
			for(auto const& value: out) {
				while(!value->push(e));
			}
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

		//A operação deve descrever a ação a tomar para a produção de 
		// um único elemento da stream **** Ainda não implementado !!!!!
		//Operação do Produtor
		virtual void operation(void) {}

		//Para controlar o fluxo de dados é necessário informar os consumidores que 
		// os produtores já não estão em produção ...
		void end()
		{
			for(auto &value: done){
				(value)->store(true);
			}
		}

		virtual void run(){
			thread *producer_thread=NULL;
			//Run executa as threads da função operação...
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