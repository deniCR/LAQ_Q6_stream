#ifndef HEADER_STREAM_COMP_H
#define HEADER_STREAM_COMP_H

#include "../../engine/include/engine.hpp"
#include "producer.hpp"
#include "join.hpp"
#include "new_join.hpp"

using namespace std;
using namespace boost;
using namespace stream;
using namespace engine;

inline bool filter_var_a(vector<Literal> args)
    { return args[0]>="1994-01-01"&&args[0]<"1995-01-01";}

inline Decimal lift_var_f_map(vector<Decimal> args)
    { return args[0]*args[1];}

/*
 * The Producer and Consumer classes are only templates 
 * for the development of stream components
*/

//Load_A1
class Load_LabelBlock :
	public virtual Producer <LabelBlock*>
{
	typedef Data_Stream_struct<LabelBlock*> Stream_data;

	private:
        ID end;
        Bitmap *var_bitmap; 

	public:
		Load_LabelBlock(Bitmap *_var_bitmap,
			Consumer<LabelBlock*> c):Producer(c)
		{
			var_bitmap = _var_bitmap;
            end = var_bitmap->nLabelBlocks;
		}

		inline void operation(void)
		{
			ID i = 0;
			for (i = 0; i < end; ++i){
	            var_bitmap->loadLabelBlock(i);
			   	send(var_bitmap->labels[i]);
			}
		}
};

//Load_A2
class Load_BitmapBlock : 
	public Producer <BitmapBlock*>
{
	typedef Data_Stream_struct<BitmapBlock*> Stream_data;

	private:
        ID end;
        Bitmap *var_bitmap; 

	public:
		Load_BitmapBlock(Bitmap *_var_bitmap,
			Consumer<BitmapBlock*> c):Producer(c)
		{
			var_bitmap = _var_bitmap;
            end = var_bitmap->nBlocks;
		}

		inline void operation(void)
		{
			ID i = 0;
			for (i = 0; i < end; ++i){
                var_bitmap->loadBlock(i);
			   	send(var_bitmap->blocks[i]);
			}
		}
};

//Load_B,Load_E,Load_C,Load_F
class Load_DecimalVectorBlock : 
	public Producer <DecimalVectorBlock*>
{
	typedef Data_Stream_struct<DecimalVectorBlock*> Stream_data;

	private:
        ID end;
        DecimalVector *var_decimalvector; 

	public:
		Load_DecimalVectorBlock(DecimalVector *_var_decimalvector,
			Consumer<DecimalVectorBlock*> c):Producer(c)
		{
			var_decimalvector = _var_decimalvector;
            end = var_decimalvector->nBlocks;
		}

		inline void operation(void)
		{
			ID i = 0;
			for (i = 0; i < end; ++i){
                var_decimalvector->loadBlock(i);
			   	send(var_decimalvector->blocks[i]);
			}
		}
};

//Dot
class Dot_BitmapBlock : 
	public Consumer<BitmapBlock*>,
	public Producer<FilteredBitVectorBlock*>
{
	typedef BitmapBlock Data;
	typedef Data_Stream_struct<BitmapBlock*> Input_data;
	typedef Data_Stream_struct<FilteredBitVectorBlock*> Output_data;
	typedef lockfree::queue<Output_data*> Channel;

	private:
        FilteredBitVector *var_a_pred;
        FilteredBitVector *var_a;

	public:
		Dot_BitmapBlock(FilteredBitVector *_var_a_pred,
			FilteredBitVector *_var_a,int threads,
			Channel *_out, boost::atomic<bool> *_done):
			Consumer(threads),Producer(_out,_done)
		{
            var_a_pred = _var_a_pred;
            var_a = _var_a;
		}

		Dot_BitmapBlock(FilteredBitVector *_var_a_pred,
			FilteredBitVector *_var_a,int threads,
			Consumer<FilteredBitVectorBlock*> next):
			Consumer(threads),Producer(next)
		{
            var_a_pred = _var_a_pred;
            var_a = _var_a;
		}

		using Consumer::run_seq;
		using Consumer::run;

		inline void operation(void)
		{
			ID id=0;
			Data *block;
			Input_data *next=NULL;

			while(pop_next(&next)){
				if(next){
					id=next->id;
		            var_a->blocks[id] = new FilteredBitVectorBlock();
		            block = static_cast<Data*>(next->data);
		            dot(*var_a_pred,*(block), var_a->blocks[id]);
		            send(id,var_a->blocks[id]);
				}
				next=NULL;
			}
		}

		/*
		 * The run function must be redefined in order to 
		 * proceed with the correct handling of the end function.
		 *
		 * The end function can only be executed at the end of threads execution.
		 *
		 * Only in cases where the execution of several instances 
		 * of the same producer occurs we must take this care.
		*/

		virtual void run() override
		{
			std::vector<boost::thread*> threads;
			int i=0;
			int n_threads=this->getMaxThreads();

			for(i=0;i<n_threads;++i){
				threads.push_back(new thread(&Consumer::operation, this));
			}

			for(auto const& consumer_thread: threads) {
				consumer_thread->join();
				delete consumer_thread;
			}

			this->end();
		}
};

//Filter_A
class Filter_LabelBlock : 
	public Consumer<LabelBlock*>
{
	typedef LabelBlock Data;
	typedef Data_Stream_struct<Data*> Input_data;

	private:
        FilteredBitVector *var_a_pred;

	public:
		Filter_LabelBlock(FilteredBitVector *_var_a_pred,
			int threads): Consumer(threads)
		{
            var_a_pred = _var_a_pred;
		}

		using Consumer::run_seq;
		using Consumer::run;

		inline void operation(void)
		{
			ID id = -1;
			Data *block;
			Input_data *next = NULL;

			while(pop_next(&next)){
				if(next!=NULL){
					id = next->id;
		            var_a_pred->blocks[id] = new FilteredBitVectorBlock();
		            block = static_cast<Data*>(next->data);
		            filter(filter_var_a,{*(block)},var_a_pred->blocks[id]);
		            next->remove();
				}
				next=NULL;
			}
		}
};

//Filter_B,Filter_C
class Filter_DecimalVectorBlock :
	public Consumer<DecimalVectorBlock*>,
	public Producer<FilteredBitVectorBlock*>
{
	typedef DecimalVectorBlock Data;
	typedef Data_Stream_struct<DecimalVectorBlock*> Input_data;
	typedef Data_Stream_struct<FilteredBitVectorBlock*> Output_data;
	typedef lockfree::queue<Output_data*> Channel;

	private:
        FilteredBitVector *var_b;
        bool(*f)(vector<Decimal>);

	public:
		Filter_DecimalVectorBlock(FilteredBitVector *_var_b,
			bool(*_f)(vector<Decimal>),int threads,Channel *_out,
			boost::atomic<bool> *_done):
			Consumer(threads),Producer(_out,_done)
		{
            var_b = _var_b;
            f = _f;
		}

		Filter_DecimalVectorBlock(FilteredBitVector *_var_b,
			bool(*_f)(vector<Decimal>),int threads,
			Consumer<FilteredBitVectorBlock*> next):
			Consumer(threads),Producer(next)
		{
            var_b = _var_b;
            f = _f;
		}

		using Consumer::run_seq;
		using Consumer::run;

		inline void operation(void)
		{
			ID id=0;
			Data *block;
			Input_data *next=NULL;

			while(pop_next(&next)){
				if(next){
					id=next->id;
					var_b->blocks[id] = new FilteredBitVectorBlock();
		            block = static_cast<Data*>(next->data);
		            filter( f,{*(block)},var_b->blocks[id]);
		            send(id,var_b->blocks[id]);
		            next->remove();
				}
				next=NULL;
			}
		}

		/*
		 * The run function must be redefined in order to 
		 * proceed with the correct handling of the end function.
		 *
		 * The end function can only be executed at the end of threads execution.
		 *
		 * Only in cases where the execution of several instances 
		 * of the same producer occurs we must take this care.
		*/

		virtual void run() override
		{
			std::vector<boost::thread*> threads;
			int i=0;
			int n_threads=this->getMaxThreads();

			for(i=0;i<n_threads;++i){
				threads.push_back(new thread(&Consumer::operation, this));
			}

			for(auto const& consumer_thread: threads) {
				consumer_thread->join();
				delete consumer_thread;
			}

			this->end();
		}
};


//Hadamard_D,Hadamard_E
class Hadamard_FilteredBitVectorBlock : 
	public Consumer<std::tuple<FilteredBitVectorBlock*,FilteredBitVectorBlock*>*>,
	public Producer<FilteredBitVectorBlock*>
{
	typedef FilteredBitVectorBlock Block;
	typedef std::tuple<FilteredBitVectorBlock*,FilteredBitVectorBlock*> Data;
	typedef Data_Stream_struct<Data*> Input_data;
	typedef Data_Stream_struct<FilteredBitVectorBlock*> Output_data;

	private:
        FilteredBitVector *var_c;
	public:
		Hadamard_FilteredBitVectorBlock(FilteredBitVector *_var_c,
			int threads,Consumer<FilteredBitVectorBlock*> next):
			Consumer(threads),Producer(next)
		{
            var_c = _var_c;
		}

		using Consumer::run_seq;
		using Consumer::run;

		inline void operation(void)
		{
			ID id=0;
			Block *block_dot,*block_filter;
			Data *block_join;
			Input_data *next=NULL;

			while(pop_next(&next)){
				if(next){
					id=next->id;
					block_join=next->data;
					var_c->blocks[id] = new Block();
		            block_join = static_cast<Data*>(next->data);
		            block_dot = static_cast<Block*>(get<0>(*block_join));
		            block_filter = static_cast<Block*>(get<1>(*block_join));
		            krao(*(block_dot),*(block_filter),var_c->blocks[id]);
		            send(id,var_c->blocks[id]);
		            next->remove();
				}
				next=NULL;
			}
			this->end();
		}
};

class Hadamard_FilteredBitVectorBlock_2 : 
	public New_Join<FilteredBitVectorBlock*,FilteredBitVectorBlock*,FilteredBitVectorBlock*>
{
	typedef Data_Stream_struct<FilteredBitVectorBlock*> Input_1;
	typedef Data_Stream_struct<FilteredBitVectorBlock*> Input_2;

	private:
        FilteredBitVector *var_c;
		ID id=0;
	public:
		Hadamard_FilteredBitVectorBlock_2(FilteredBitVector *_var_c,
			int threads,Consumer<FilteredBitVectorBlock*> next):
			New_Join(threads,next)
		{
            var_c = _var_c;
		}
		Hadamard_FilteredBitVectorBlock_2(FilteredBitVector *_var_c,
			int threads,Consumer<FilteredBitVectorBlock*> next, string _name):
			New_Join(threads,next,_name)
		{
            var_c = _var_c;
		}

		using New_Join::exec;

		inline void exec (Input_1 *in1, Input_2 *in2)
		{
			if(in1 && in2){
				id=in1->id;
				var_c->blocks[id] = new FilteredBitVectorBlock();
		        krao(*(in1->data),*(in2->data),var_c->blocks[id]);
		        send(id,var_c->blocks[id]);
		        in1->remove();
		        in2->remove();
			}
		}
};

//Hadamard_
class Hadamard_DecimalVectorBlock : 
	public Consumer<std::tuple<FilteredBitVectorBlock*,DecimalVectorBlock*>*>,
	public Producer<FilteredDecimalVectorBlock*>
{
	typedef DecimalVectorBlock Block_map;
	typedef FilteredBitVectorBlock Block_had;
	typedef std::tuple<Block_had*,Block_map*> Data;
	typedef Data_Stream_struct<Data*> Input_data;
	typedef Data_Stream_struct<FilteredDecimalVectorBlock*> Output_data;

	private:
        FilteredDecimalVector *var_g;
	public:
		Hadamard_DecimalVectorBlock(FilteredDecimalVector *_var_g,
			int threads,Consumer<FilteredDecimalVectorBlock*> next):
			Consumer(threads),Producer(next)
		{
            var_g = _var_g;
		}

		using Consumer::run_seq;
		using Consumer::run;

		inline void operation(void)
		{
			ID id=0;
			Block_had *block_had;
			Block_map *block_map;
			Data *block_join;
			Input_data *next=NULL;

			while(pop_next(&next)){
				if(next){
					id=next->id;
					var_g->blocks[id] = new FilteredDecimalVectorBlock();
		            block_join = static_cast<Data*>(next->data);
		            block_had = static_cast<Block_had*>(get<0>(*block_join));
		            block_map = static_cast<Block_map*>(get<1>(*block_join));
		            krao(*(block_had),*(block_map),var_g->blocks[id]);
		            send(id,var_g->blocks[id]);
		            next->remove();
				}
				next=NULL;
			}
			this->end();
		}
};

class Hadamard_DecimalVectorBlock_2 : 
	public New_Join<FilteredBitVectorBlock*,DecimalVectorBlock*,FilteredDecimalVectorBlock*>
{
	typedef Data_Stream_struct<FilteredBitVectorBlock*> Input_1;
	typedef Data_Stream_struct<DecimalVectorBlock*> Input_2;

	private:
        FilteredDecimalVector *var_g;
		ID id=0;
	public:
		Hadamard_DecimalVectorBlock_2(FilteredDecimalVector *_var_g,
			int threads,Consumer<FilteredDecimalVectorBlock*> next):
			New_Join(threads,next)
		{
            var_g = _var_g;
		}
		Hadamard_DecimalVectorBlock_2(FilteredDecimalVector *_var_g,
			int threads,Consumer<FilteredDecimalVectorBlock*> next, string _name):
			New_Join(threads,next,_name)
		{
            var_g = _var_g;
		}

		inline void exec (Input_1 *in1, Input_2 *in2)
		{
			if(in1 && in2){
				id=in1->id;
				var_g->blocks[id] = new FilteredDecimalVectorBlock();
		        krao(*(in1->data),*(in2->data),var_g->blocks[id]);
		        send(id,var_g->blocks[id]);
		        in1->remove();
		        in2->remove();
			}
		}
};

class Map : 
	public Consumer<std::tuple<DecimalVectorBlock*,DecimalVectorBlock*>*>,
	public Producer<DecimalVectorBlock*>
{
	typedef DecimalVectorBlock Block;
	typedef std::tuple<Block*,Block*> Data;
	typedef Data_Stream_struct<Data*> Input_data;
	typedef Data_Stream_struct<DecimalVectorBlock*> Output_data;
	typedef lockfree::queue<Output_data*> Channel;

	private:
        DecimalVector *var_f;

	public:
		Map(DecimalVector *_var_f,int threads,
			Channel *_out,
			boost::atomic<bool> *_done):
			Consumer(threads),Producer(_out,_done)
		{
            var_f = _var_f;
		}

		Map(DecimalVector *_var_f,int threads,
			Consumer<DecimalVectorBlock*> next):
			Consumer(threads),Producer(next)
		{
            var_f = _var_f;
		}

		using Consumer::operation;
		using Consumer::run_seq;
		using Consumer::run;		

		inline void operation(void)
		{
			ID id=0;
			Block *block_b,*block_f;
			Data *join_block;
			DecimalVectorBlock *block_id;
			Input_data *next=NULL;

			while(pop_next(&next)){
				if(next){
					id=next->id;
					join_block=next->data;
		            block_b = static_cast<Block*>(get<1>(*join_block));
		            block_f = static_cast<Block*>(get<0>(*join_block));
		            block_id = new DecimalVectorBlock();
		            lift(lift_var_f_map,{*(block_f),*(block_b)},block_id);
		            send(id,block_id);
		            block_id = NULL;
		            block_f = NULL;
		            block_b = NULL; 
		            next->remove();
				}
				next=NULL;
			}

			this->end();
		}
};

class Map_2 : 
	public New_Join<DecimalVectorBlock*, DecimalVectorBlock*, DecimalVectorBlock*>
{
	typedef Data_Stream_struct<DecimalVectorBlock*> Input_1;
	typedef Data_Stream_struct<DecimalVectorBlock*> Input_2;
	typedef Data_Stream_struct<DecimalVectorBlock*> Output;
	typedef lockfree::queue<Output*> Channel;

	private:
        DecimalVector *var_f;
		ID id=0;
		DecimalVectorBlock *block_id;

	public:
		Map_2(DecimalVector *_var_f,int threads,
			Channel *_out,boost::atomic<bool> *_done):
			New_Join(threads,_out,_done)
		{
            var_f = _var_f;
		}
		Map_2(DecimalVector *_var_f,int threads,
			Channel *_out,boost::atomic<bool> *_done,
			string _name):
			New_Join(threads,_out,_done,_name)
		{
            var_f = _var_f;
		}
		Map_2(DecimalVector *_var_f,int threads,
			Consumer<DecimalVectorBlock*> next):
			New_Join(threads,next)
		{
            var_f = _var_f;
		}
		Map_2(DecimalVector *_var_f,int threads,
			Consumer<DecimalVectorBlock*> next, string _name):
			New_Join(threads,next,_name)
		{
            var_f = _var_f;
		}

		using Consumer::operation;
		using Consumer::run_seq;
		using Consumer::run;

		inline void exec (Input_1 *in1, Input_2 *in2)
		{
			if(in1 && in2){
				id=in1->id;
		        block_id = new DecimalVectorBlock();
		        lift(lift_var_f_map,{*(in1->data),*(in2->data)},block_id);
		        send(id,block_id);
		        in1->remove();
		        in2->remove();
		        block_id=NULL;
			}
		}
};


class Sum : 
	public Consumer<FilteredDecimalVectorBlock*>
{
	typedef FilteredDecimalVectorBlock Data;
	typedef Data_Stream_struct<Data*> Input_data;

	private:
        Decimal *var_h;

	public:
		Sum(Decimal *_var_h,int threads): Consumer(threads)
		{
            var_h = _var_h;
		}

		inline void operation(void)
		{
			ID id=0;
			Data *block;
			Input_data *next=NULL;

			while(pop_next(&next)){
				if(next){
					id=next->id;
		            block = static_cast<FilteredDecimalVectorBlock*>(next->data);
		            sum(*block, var_h);
		            next->remove();
				}
				next=NULL;
			}
		}
};

#endif