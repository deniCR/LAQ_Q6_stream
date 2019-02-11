#ifndef STREAM_DATA_STREAM_TYPE_H
#define STREAM_DATA_STREAM_TYPE_H

#include <boost/atomic.hpp>

namespace stream {

	typedef long ID;
	
	template <typename T>
	struct Data_Stream_struct {
		ID id;
		T data;
		boost::atomic<long> delete_counter;

		Data_Stream_struct(ID _id, T _data)
			:id(_id),data(_data),delete_counter(0)
		{}

		Data_Stream_struct(ID _id, T _data, long _delete_counter)
			:id(_id),data(_data),delete_counter(_delete_counter)
		{}

		~Data_Stream_struct()
		{
			delete data;
		}

		void remove(){
			if(delete_counter<=1)
			{
				delete this;
			}
			else
			{
				--delete_counter;
			}
		}
	};
}

#endif