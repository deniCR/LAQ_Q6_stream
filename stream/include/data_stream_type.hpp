/*
 * Copyright (c) 2019 Daniel Rodrigues. All rights reserved.
 */
#ifndef STREAM_DATA_STREAM_TYPE_H
#define STREAM_DATA_STREAM_TYPE_H

#include <iostream>
#include <boost/thread/mutex.hpp>
#include <boost/atomic.hpp>


namespace stream {

	typedef long ID;
	

	template <typename T>
	struct Data_Stream_struct {
	private:
		boost::atomic<long> delete_counter;
	public:
		ID id;
		T data;

		Data_Stream_struct(ID _id, T _data)
			:id(_id),data(_data)
		{
			delete_counter.store(1, boost::memory_order_seq_cst);
		}

		Data_Stream_struct(ID _id, T _data, long _delete_counter)
			:id(_id),data(_data)
		{
			delete_counter.store(_delete_counter, boost::memory_order_seq_cst);
		}

		~Data_Stream_struct()
		{
			//delete data;
		}

		void remove(){

			int delete_c_1 = delete_counter.load(boost::memory_order_seq_cst);
			delete_counter.fetch_add(-1, boost::memory_order_seq_cst);
			int delete_c_2 = delete_counter.load(boost::memory_order_seq_cst);

			if(delete_c_2<=0)
			{
				free(data);
			}
		}
	};
}


/** Versão com locks ... 
	template <typename T>
	struct Data_Stream_struct {
	private:
		int delete_c_m=1;
		bool delete_f=false;
		boost::atomic<int> delete_counter{1};
		boost::mutex delete_m;

	public:
		ID id=0;
		T data=NULL;

		Data_Stream_struct(ID _id, T _data)
			:id(_id),data(_data)
		{
			delete_counter.store(1, boost::memory_order_seq_cst);
			delete_c_m = 1;
		}

		Data_Stream_struct(ID _id, T _data, int _delete_counter)
			:id(_id),data(_data),delete_c_m(_delete_counter)
		{
			delete_counter.store(_delete_counter, boost::memory_order_seq_cst);
		}

		~Data_Stream_struct()
		{
			std::cout << "----- Delete element ..." << std::endl;
		}

		void remove(){
			delete_m.lock();
			{
				--delete_c_m;
				int delete_c = delete_c_m;
				//delete_counter.fetch_add(-1, boost::memory_order_seq_cst);
				//int delete_c = delete_counter.load(boost::memory_order_seq_cst);
				if(delete_c==0)
				{
					if(data){
						free(data);
						data=NULL;
					}
				}
			}
			delete_m.unlock();
		}
	};
}
**/

#endif