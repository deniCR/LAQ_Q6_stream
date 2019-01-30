#ifndef HEADER_STREAM_COMP_H
#define HEADER_STREAM_COMP_H

#include "../../engine/include/engine.hpp"
#include "producer.hpp"
#include "join.hpp"

using namespace std;
using namespace boost;
using namespace stream;
using namespace engine;

inline bool filter_var_a(vector<Literal> args)
    { return args[0]>="1994-01-01"&&args[0]<"1995-01-01";}

inline Decimal lift_var_f_map(vector<Decimal> args)
    { return args[0]*args[1];}

//Produtor
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

//Load_BE,Load_C,Load_F
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

//Produtor e Consumidor
//Dot_A
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

		inline void operation(void)
		{
			ID id = 0;
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
					block_join=next->data;
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

		using Consumer::run_seq;

		inline void operation(void)
		{
			ID id=0;
			Block *block_b,*block_f;
			Data *join_block;
			Input_data *next=NULL;

			while(pop_next(&next)){
				if(next){
					id=next->id;
					join_block=next->data;
		            block_b = static_cast<Block*>(get<1>(*join_block));
		            block_f = static_cast<Block*>(get<0>(*join_block));
		            var_f->blocks[id] = new DecimalVectorBlock();
		            lift(lift_var_f_map,{*(block_f),*(block_b)},var_f->blocks[id]);
		            send(id,var_f->blocks[id]);
		            next->remove();
				}
				next=NULL;
			}
			this->end();
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
			Data *block;
			Input_data *next=NULL;

			while(pop_next(&next)){
				if(next){
		            block = static_cast<FilteredDecimalVectorBlock*>(next->data);
		            sum(*block, var_h);
		            next->remove();
				}
				next=NULL;
			}
		}
};

#endif
