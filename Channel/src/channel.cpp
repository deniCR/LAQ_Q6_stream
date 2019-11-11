#include "../include/channel.hpp"

namespace channel {

}

/*
	void *Channel::pop_lockFree()
	{
		long index=0;
		void *result=NULL;

		index = consumer_index_atomic.fetch_add(1,boost::memory_order_seq_cst);

		if(index>=size){
			consumer_index_atomic.store(0,boost::memory_order_seq_cst);
		}
		else {
			//std::cout << "Index: " << index << " Size: " << size
			//	<< " producer_index: " << producer_index << std::endl;

			if(index<=producer_index && (result=array[index])!=NULL)
			{
				consumer_count_atomic.fetch_add(1,boost::memory_order_seq_cst);
				array[index]=NULL;
			}
			else
				consumer_index_atomic.fetch_sub(1,boost::memory_order_seq_cst);
		}

		return	result;
	}

	bool Channel::forcePop_lockFree(void *elem)
	{
		bool r=false;
		if(ready()){
			for(int i=0; i<5 && (elem=pop_lockFree())==NULL; ++i){
				std::this_thread::sleep_for(std::chrono::nanoseconds(3000));
			}
		}

		if(elem!=NULL)
			r=true;
		return r;
	}

	*/