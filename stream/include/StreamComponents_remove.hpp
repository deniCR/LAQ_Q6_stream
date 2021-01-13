/*
 * Copyright (c) 2019 Daniel Rodrigues. All rights reserved.
 */
#ifndef HEADER_STREAM_COMP_H
#define HEADER_STREAM_COMP_H

#include "engine.hpp"
#include "producer.hpp"
#include "consumer_producer.hpp"
#include "join.hpp"

using namespace std;
using namespace boost;
using namespace stream;
using namespace engine;

/*
 * The Producer, Consumer, Join and Consumer-Producer classes are only templates 
 * for the development of stream components
*/

//Load_A1
class Load_LabelBlock :
	public virtual Producer <LabelBlock>
{
	typedef Data_Stream_struct<LabelBlock> Stream_data;

	private:
        ID end;
        Bitmap *var_bitmap;

	public:
		Load_LabelBlock(Bitmap *_var_bitmap)
			: Producer()
		{
			var_bitmap = _var_bitmap;
            end = var_bitmap->nLabelBlocks;
		}

		Load_LabelBlock(long _max_threads, Bitmap *_var_bitmap,
			long _n_executions)
			: Producer(_max_threads,_n_executions)
		{
			var_bitmap = _var_bitmap;
            end = var_bitmap->nLabelBlocks;
		}

		#if defined(D_PAPI) || defined(D_PAPI_OPS)
			Load_LabelBlock(long _max_threads, Bitmap *_var_bitmap,
				long _n_executions, int papi_op)
				: Producer(_max_threads,_n_executions,papi_op)
			{
				var_bitmap = _var_bitmap;
	            end = var_bitmap->nLabelBlocks;
			}
		#endif

		inline void operation(void)
		{
			ID i = 0;
			for (i = 0; i < end; ++i){
  				Stream_data *l = getSinglePush();
 	        	var_bitmap->loadLabelBlock(i,l->data);
			   	send(i,l);
			}
		}

		inline void operation(long id){
  			Stream_data *l = getSinglePush();
 	        var_bitmap->loadLabelBlock(id,l->data);
			send(id,l);
		}
};

//Load_A2
class Load_BitmapBlock : 
	public Producer <BitmapBlock>
{
	typedef Data_Stream_struct<BitmapBlock> Stream_data;

	private:
        ID end;
        Bitmap *var_bitmap; 
        std::string db_path,db_name,db_tabel,db_label;

	public:
		Load_BitmapBlock(Bitmap *_var_bitmap)
			:Producer()
		{
			var_bitmap = _var_bitmap;
            end = var_bitmap->nBlocks;
		}

		Load_BitmapBlock(int _max_threads, Bitmap *_var_bitmap,
			long _n_executions )
			:Producer(_max_threads,_n_executions)
		{
			var_bitmap = _var_bitmap;
            end = var_bitmap->nBlocks;
		}

		#if defined(D_PAPI) || defined(D_PAPI_OPS)
			Load_BitmapBlock(int _max_threads, Bitmap *_var_bitmap,
				long _n_executions, int papi_op)
				:Producer(_max_threads,_n_executions,papi_op)
			{
				var_bitmap = _var_bitmap;
	            end = var_bitmap->nBlocks;
			}

			Load_BitmapBlock(int _max_threads, std::string _db_path,
				std::string _db_name,std::string _db_tabel,std::string _db_label,
				long _n_executions, int papi_op)
				: Producer(_max_threads,_n_executions,papi_op)
			{
				var_bitmap=NULL;
				db_path=_db_path; db_name=_db_name; db_tabel=_db_tabel; db_label=_db_label;
			}
		#endif

		inline void init(void){
			if(var_bitmap==NULL){
				  var_bitmap =
  					new engine::Bitmap(db_path,
					    db_name,db_tabel,db_label);
             	end = var_bitmap->nBlocks;
			}
		}

		inline void operation(void)
		{

			ID i = 0;
			for (i = 0; i < end; ++i){
				Stream_data *b = getSinglePush();
                var_bitmap->loadBlock(i,b->data);
			   	send(i,b);
			}
		}

		inline void operation(long id){
			Stream_data *b = getSinglePush();
            var_bitmap->loadBlock(id,b->data);
			send(id,b);
		}
};

//Load_B,Load_E,Load_C,Load_F
class Load_DecimalVectorBlock : 
	public Producer <DecimalVectorBlock>
{
	typedef Data_Stream_struct<DecimalVectorBlock> Stream_data;
	typedef channel::Channel<DecimalVectorBlock> Channel;

	private:
        ID end;
        DecimalVector *var_decimalvector;
        std::string db_path,db_name,db_tabel,db_label;

	public:
		Load_DecimalVectorBlock(DecimalVector *_var_decimalvector)
			:Producer()
		{
			var_decimalvector = _var_decimalvector;
            end = var_decimalvector->nBlocks;
		}

		Load_DecimalVectorBlock(int _max_threads, DecimalVector *_var_decimalvector,
			long _n_executions)
			: Producer(_max_threads,_n_executions)
		{
			var_decimalvector = _var_decimalvector;
            end = var_decimalvector->nBlocks;
		}

		Load_DecimalVectorBlock(int _max_threads,
				std::string _db_path,std::string _db_name,
				std::string _db_tabel,std::string _db_label,
				long _n_executions)
			: Producer(_max_threads,_n_executions)
		{
			var_decimalvector=NULL;
			db_path=_db_path; db_name=_db_name; db_tabel=_db_tabel; db_label=_db_label;
		}

		#if defined(D_PAPI) || defined(D_PAPI_OPS)
			Load_DecimalVectorBlock(int _max_threads,
					std::string _db_path,std::string _db_name,
					std::string _db_tabel,std::string _db_label,
				long _n_executions, int papi_op)
				: Producer(_max_threads,_n_executions,papi_op)
			{
				var_decimalvector=NULL;
				db_path=_db_path; db_name=_db_name; db_tabel=_db_tabel; db_label=_db_label;
			}
		#endif

		inline void init(void){
			if(var_decimalvector==NULL){
				  var_decimalvector =
  					new engine::DecimalVector(db_path,
					    db_name,db_tabel,db_label);
             	end = var_decimalvector->nBlocks;
			}
		}

		inline void operation(void)
		{
			ID i = 0;
			for (i = 0; i < end; ++i){
				Stream_data *b = getSinglePush();
                var_decimalvector->loadBlock(i,b->data);
			   	send(i,b);
			}
		}

		inline void operation(long id){
			Stream_data *b = getSinglePush();
            var_decimalvector->loadBlock(id,b->data);
			send(id,b);
		}
};

class Dot_BitmapBlock_2 : 
	public Consumer_Producer<BitmapBlock,FilteredBitVectorBlock>
{
	typedef BitmapBlock Data;
	typedef Data_Stream_struct<BitmapBlock> Input_data;
	typedef Data_Stream_struct<FilteredBitVectorBlock> Output_data;
	typedef channel::Channel<FilteredBitVectorBlock> Channel;


	private:
        FilteredBitVector *var_a_pred;
        boost::mutex pop_mutex;

	public:

		Dot_BitmapBlock_2(FilteredBitVector *_var_a_pred,
			long threads,Producer<BitmapBlock> *prev)
			: Consumer_Producer(threads,prev)
		{
            var_a_pred = _var_a_pred;
		}

		#if defined(D_PAPI) || defined(D_PAPI_OPS)
			Dot_BitmapBlock_2(FilteredBitVector *_var_a_pred,
				long threads,Producer<BitmapBlock> *prev, int papi_op)
				: Consumer_Producer(threads,papi_op,prev)
			{
	            var_a_pred = _var_a_pred;
			}
		#endif

		inline void operation(void) override
		{
			ID id=0;
			Output_data *block=NULL;
			Input_data *next=NULL;
			bool pop_r=true;

			do {
				pop_r = pop_next(&next);
				if(next){
					id=next->id;
		            block = getSinglePush();
		            dot(*var_a_pred,*(next->data), block->data);
		            next->remove();
		            send(id,block);
		            block = NULL;
				}
				next=NULL;
			} while(pop_r);
		}
};

class Dot_BitmapBlock : 
	public Consumer<BitmapBlock>
{
	typedef BitmapBlock Data;
	typedef Data_Stream_struct<BitmapBlock> Input_data;
	typedef Data_Stream_struct<FilteredBitVectorBlock> Output_data;


	private:
        FilteredBitVector *var_a_pred;
        FilteredBitVector *var_d;

	public:
		Dot_BitmapBlock(FilteredBitVector *_var_a_pred,
			long threads, Producer<BitmapBlock> *prev,FilteredBitVector *_var_d)
			: Consumer(threads,prev)
		{
            var_a_pred = _var_a_pred;
            var_d=_var_d;
		}

		#if defined(D_PAPI) || defined(D_PAPI_OPS)
			Dot_BitmapBlock(FilteredBitVector *_var_a_pred,
				long threads, Producer<BitmapBlock> *prev,FilteredBitVector *_var_d, int papi_op)
				: Consumer(threads,prev,papi_op)
			{
	            var_a_pred = _var_a_pred;
	            var_d=_var_d;
			}
		#endif

		inline void operation(void) override
		{
			ID id=0;
			Input_data *next=NULL;
			bool pop_r=true;

			do {
				pop_r = pop_next(&next);

				if(next){
					id=next->id;
		            var_d->blocks[id] = new FilteredBitVectorBlock();
		            dot(*var_a_pred,*(next->data), var_d->blocks[id]);
		            next->remove();
				}
				next=NULL;
			} while(pop_r);
		}
};

//Filter_A
class Filter_LabelBlock : 
	public Consumer<LabelBlock>
{
	typedef LabelBlock Data;
	typedef Data_Stream_struct<Data> Input_data;

	private:
        FilteredBitVector *var_a_pred;
       	bool (*f)(vector<Literal> *);

	public:
		Filter_LabelBlock(bool (*_f)(vector<Literal> *),
			FilteredBitVector *_var_a_pred, long threads,
			Producer<LabelBlock> *prev)
			: Consumer(threads,prev)
		{
			f=_f;
            var_a_pred = _var_a_pred;
		}

		#if defined(D_PAPI) || defined(D_PAPI_OPS)
			Filter_LabelBlock(bool (*_f)(vector<Literal> *),
				FilteredBitVector *_var_a_pred, long threads,
				Producer<LabelBlock> *prev, int papi_op)
				: Consumer(threads,prev,papi_op)
			{
				f=_f;
	            var_a_pred = _var_a_pred;
			}
		#endif

		using Consumer::run_seq;
		using Consumer::run;

		inline void operation(void)
		{
			ID id = 0;
			ID count = 0;
			Input_data *next = NULL;

			while(pop_next(&next)){
				if(next){
					count++;
					id = next->id;
		            var_a_pred->blocks[id] = new FilteredBitVectorBlock();
		            filter(f,next->data,var_a_pred->blocks[id]);
		            next->remove();
				}
				next=NULL;
			}
		}
};

class Filter_DecimalVectorBlock_2 :
	public Consumer_Producer<DecimalVectorBlock,FilteredBitVectorBlock>
{
	typedef DecimalVectorBlock Data;
	typedef Data_Stream_struct<DecimalVectorBlock> Input_data;
	typedef Data_Stream_struct<FilteredBitVectorBlock> Output_data;
	typedef channel::Channel<FilteredBitVectorBlock> Channel;

	private:
        bool(*f)(vector<Decimal> *);
        boost::mutex pop_mutex;

	public:
		Filter_DecimalVectorBlock_2(bool(*_f)(vector<Decimal> *),
			long threads,Producer<DecimalVectorBlock> *prev)
			: Consumer_Producer(threads,prev)
		{
            f = _f;
		}

		#if defined(D_PAPI) || defined(D_PAPI_OPS)
			Filter_DecimalVectorBlock_2(bool(*_f)(vector<Decimal> *),
				long threads,Producer<DecimalVectorBlock> *prev,int papi_op)
				: Consumer_Producer(threads,papi_op,prev)
			{
	            f = _f;
			}
		#endif

		inline void operation(void)
		{
			ID id=0;
			FilteredBitVectorBlock *id_block=NULL;
			Output_data *block=NULL;
			Input_data *next=NULL;
			bool pop_r=true;

			do {
				pop_r = pop_next(&next);
				if(next){
					id=next->id;
					block = getSinglePush();
		            filter(f,next->data,block->data);
		            next->remove();
		            send(id,block);
		            block=NULL;
				}
				next=NULL;
			} while(pop_r);
		}
};

class Hadamard_FilteredBitVectorBlock_2 : 
	public Join<FilteredBitVectorBlock,FilteredBitVectorBlock,FilteredBitVectorBlock>
{
	typedef Data_Stream_struct<FilteredBitVectorBlock> Input_1;
	typedef Data_Stream_struct<FilteredBitVectorBlock> Input_2;
	typedef Data_Stream_struct<FilteredBitVectorBlock> Output;

	public:
		Hadamard_FilteredBitVectorBlock_2(long threads,
			Producer<FilteredBitVectorBlock> *prev_1,
			Producer<FilteredBitVectorBlock> *prev_2)
			: Join(threads,prev_1,prev_2)
		{}

		Hadamard_FilteredBitVectorBlock_2(long threads,
			Producer<FilteredBitVectorBlock> *prev_1,
			Producer<FilteredBitVectorBlock> *prev_2, string _name)
			: Join(threads,prev_1,prev_2,_name)
		{}

		#if defined(D_PAPI) || defined(D_PAPI_OPS)
			Hadamard_FilteredBitVectorBlock_2(long threads,
				Producer<FilteredBitVectorBlock> *prev_1,
				Producer<FilteredBitVectorBlock> *prev_2, string _name, int papi_op)
				: Join(threads,prev_1,prev_2,_name,papi_op)
			{}
		#endif

		using Join::exec;

		inline void exec (Input_1 *in1, Input_2 *in2)
		{
			ID id=0;
			Output *block=NULL;

			if(in1 && in2){

				id=in1->id;
				block = getSinglePush();
		        krao(*(in1->data),*(in2->data),block->data);
		        in1->remove();in2->remove();
		        send(id,block);
		        block = NULL;
			}
			in1=NULL;in2=NULL;
		}
};

class Hadamard_DecimalVectorBlock_2 : 
	public Join<FilteredBitVectorBlock,DecimalVectorBlock,FilteredDecimalVectorBlock>
{
	typedef Data_Stream_struct<FilteredBitVectorBlock> Input_1;
	typedef Data_Stream_struct<DecimalVectorBlock> Input_2;
	typedef Data_Stream_struct<FilteredDecimalVectorBlock> Output;

	public:
		Hadamard_DecimalVectorBlock_2(FilteredDecimalVector *_var_g,
			long threads,Producer<FilteredBitVectorBlock> *prev_1,
			Producer<DecimalVectorBlock> *prev_2)
			: Join(threads,prev_1,prev_2)
		{}

		Hadamard_DecimalVectorBlock_2(long threads,
			Producer<FilteredBitVectorBlock> *prev_1,
			Producer<DecimalVectorBlock> *prev_2, string _name)
			: Join(threads,prev_1,prev_2,_name)
		{}

		#if defined(D_PAPI) || defined(D_PAPI_OPS)
			Hadamard_DecimalVectorBlock_2(long threads,
				Producer<FilteredBitVectorBlock> *prev_1,
				Producer<DecimalVectorBlock> *prev_2, string _name,int papi_op)
				: Join(threads,prev_1,prev_2,_name,papi_op)
			{}
		#endif

		inline void exec (Input_1 *in1, Input_2 *in2)
		{
			ID id=0;
			Output *block=NULL;

			if(in1 && in2){

				id=in1->id;
				block = getSinglePush();
		        krao(*(in1->data),*(in2->data),block->data);
		        in1->remove();in2->remove();
		        send(id,block);
		        block = NULL;
			}
			in1=NULL;in2=NULL;
		}
};

class Hadamard_DecimalVectorBlock_3 : 
	public Join<FilteredDecimalVectorBlock,FilteredBitVectorBlock,FilteredDecimalVectorBlock>
{
	typedef Data_Stream_struct<FilteredDecimalVectorBlock> Input_1;
	typedef Data_Stream_struct<FilteredBitVectorBlock> Input_2;
	typedef Data_Stream_struct<FilteredDecimalVectorBlock> Output;

	public:
		Hadamard_DecimalVectorBlock_3(long threads,
			Producer<FilteredDecimalVectorBlock> *prev_1,
			Producer<FilteredBitVectorBlock> *prev_2, string _name)
			: Join(threads,prev_1,prev_2,_name)
		{}

		#if defined(D_PAPI) || defined(D_PAPI_OPS)
			Hadamard_DecimalVectorBlock_3(long threads,
				Producer<FilteredDecimalVectorBlock> *prev_1,
				Producer<FilteredBitVectorBlock> *prev_2, string _name,int papi_op)
				: Join(threads,prev_1,prev_2,_name,papi_op)
			{}
		#endif

		inline void exec (Input_1 *in1, Input_2 *in2)
		{
			ID id=0;
			Output *block=NULL;

			if(in1 && in2){

				id=in1->id;
				block = getSinglePush();
		        krao(*(in1->data),*(in2->data),block->data);
		        in1->remove();in2->remove();
		        send(id,block);
		        block = NULL;
			}
			in1=NULL;in2=NULL;
		}
};


class Map_2 : 
	public Join<DecimalVectorBlock, DecimalVectorBlock, DecimalVectorBlock>
{
	typedef Data_Stream_struct<DecimalVectorBlock> Input_1;
	typedef Data_Stream_struct<DecimalVectorBlock> Input_2;
	typedef Data_Stream_struct<DecimalVectorBlock> Output;
	typedef channel::Channel<DecimalVectorBlock> Channel;

	private:
		Decimal (*f)(vector<Decimal> *);

	public:
		Map_2(Decimal (*_f)(vector<Decimal> *),long threads,
			Producer<DecimalVectorBlock> *prev_1,
			Producer<DecimalVectorBlock> *prev_2)
			: Join(threads,prev_1,prev_2)
		{
			f=_f;
		}

		Map_2(Decimal (*_f)(vector<Decimal> *),long threads,
			Producer<DecimalVectorBlock> *prev_1,
			Producer<DecimalVectorBlock> *prev_2,string _name)
			: Join(threads,prev_1,prev_2,_name)
		{
			f=_f;
		}

		#if defined(D_PAPI) || defined(D_PAPI_OPS)
			Map_2(Decimal (*_f)(vector<Decimal> *),long threads,
				Producer<DecimalVectorBlock> *prev_1,
				Producer<DecimalVectorBlock> *prev_2,string _name, int papi_op)
				: Join(threads,prev_1,prev_2,_name,papi_op)
			{
				f=_f;
			}
		#endif

		inline void exec (Input_1 *in1, Input_2 *in2)
		{
			ID id=0;
			Output *block=NULL;
			std::vector<DecimalVectorBlock*> aux(2);

			if(in1 && in2){

				id=in1->id;
		        block = getSinglePush();
		        aux[0]=(in1->data);
		        aux[1]=(in2->data);
		        lift(f,&aux,block->data);
		        in1->remove();in2->remove();
		        send(id,block);
		        block=NULL;
			}
			in1=NULL;in2=NULL;
		}
};


class Sum : 
	public Consumer<FilteredDecimalVectorBlock>
{
	typedef FilteredDecimalVectorBlock Data;
	typedef Data_Stream_struct<Data> Input_data;

	private:
        Decimal *var_h;
        boost::mutex sum_mutex;

	public:
		Sum(Decimal *_var_h,long threads,
			Producer<FilteredDecimalVectorBlock> *prev)
			: Consumer(threads,prev)
		{
            var_h = _var_h;
		}

		#if defined(D_PAPI) || defined(D_PAPI_OPS)
			Sum(Decimal *_var_h,long threads,
				Producer<FilteredDecimalVectorBlock> *prev, int papi_op)
				: Consumer(threads,prev,papi_op)
			{
	            var_h = _var_h;
			}
		#endif

		inline void operation(void)
		{
			Data *block;
			Decimal var_aux=0;
			Input_data *next=NULL;
			int id=0;

			while(pop_next(&next)){
				if(next){
					++id;

		            block = static_cast<FilteredDecimalVectorBlock*>(next->data);
		            var_aux+=sum(*block);
		            next->remove();
		            block = NULL;
				}
				next=NULL;
			}

			sum_mutex.lock();
			(*var_h)+=var_aux;
			sum_mutex.unlock();
		}
};

#endif