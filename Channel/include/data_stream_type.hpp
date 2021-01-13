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
		boost::atomic<int> delete_counter;
		boost::atomic<int> deleted;
		boost::atomic<int> empty_count;
	public:
		//Stream element identifier
		ID id;
		//Element dataset
		T *data=NULL;
		//Dataset state
		bool empty=true;

		Data_Stream_struct(ID _id, T *_data)
			:id(_id),data(_data),empty(true),deleted(0),empty_count(0),delete_counter(1)
		{}

		Data_Stream_struct(ID _id, T *_data, int _delete_counter)
			:id(_id),data(_data),empty(true),deleted(0),empty_count(0),delete_counter(_delete_counter)
		{}

		Data_Stream_struct(ID _id, int _delete_counter)
			:id(_id),empty(true),deleted(0),empty_count(0),delete_counter(_delete_counter)
		{
			data = new T();
		}

		Data_Stream_struct()
			:id(0),empty(true),deleted(0),empty_count(0),delete_counter(1)
		{
			data = new T();
		}

		~Data_Stream_struct()
		{
			empty=true;
		}

		inline bool clear()
		{	
			int del_before = delete_counter.fetch_add(-1, boost::memory_order_seq_cst);
			if(del_before==1 && !empty)
			{
				empty=true;
				return true;
			}
			return false;
		}

		inline void newData(ID _id, int _delete_counter)
		{
			delete_counter.store(_delete_counter, boost::memory_order_seq_cst);
			deleted.store(0, boost::memory_order_seq_cst);
			empty_count.store(0, boost::memory_order_seq_cst);
			empty=false;
			id = _id;
		}

		inline void remove()
		{
			int deleted_before = deleted.fetch_add(1, boost::memory_order_seq_cst);
			if(deleted_before==1)
			{	
				delete data;
			}
		}

		inline void erase()
		{
			int delete_ = delete_counter.fetch_add(-1, boost::memory_order_seq_cst);
			if(delete_==1)
			{
				free(data);
			}
		}

	};
}


/** Lock version --- Deprecated
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