#ifndef HEADER_STREAM_COMP_H
#define HEADER_STREAM_COMP_H

#include "../../engine/include/engine.hpp"
#include "include/producer.hpp"
#include "include/join.hpp"

using namespace std;
using namespace boost;
using namespace stream;
using namespace engine;

inline bool filter_var_a(vector<Literal> args)
    { return args[0]>="1994-01-01"&&args[0]<"1995-01-01";}

inline Decimal lift_var_f_map(vector<Decimal> args)
    { return args[0]*args[1];}

//As calsses Producer e Consumer são apenas templates para desenvolver
// os componentes do stream

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
		Load_LabelBlock(Bitmap *_var_bitmap,Consumer<LabelBlock*> c);

		inline void operation(void);
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
		Load_BitmapBlock(Bitmap *_var_bitmap,Consumer<BitmapBlock*> c);

		inline void operation(void);
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
			Consumer<DecimalVectorBlock*> c);

		inline void operation(void);
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
			Channel *_out, boost::atomic<bool> *_done);

		Dot_BitmapBlock(FilteredBitVector *_var_a_pred,
			FilteredBitVector *_var_a,int threads,
			Consumer<FilteredBitVectorBlock*> next);

		using Consumer::run_seq;

		inline void operation(void);
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
			int threads);

		inline void operation(void);
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
			boost::atomic<bool> *_done);

		Filter_DecimalVectorBlock(FilteredBitVector *_var_b,
			bool(*_f)(vector<Decimal>),int threads,
			Consumer<FilteredBitVectorBlock*> next);

		using Consumer::run_seq;

		inline void operation(void);
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
			int threads,Consumer<FilteredBitVectorBlock*> next);

		using Consumer::run_seq;

		inline void operation(void);
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
			int threads,Consumer<FilteredDecimalVectorBlock*> next);

		using Consumer::run_seq;

		inline void operation(void);
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
			boost::atomic<bool> *_done);

		Map(DecimalVector *_var_f,int threads,
			Consumer<DecimalVectorBlock*> next);

		using Consumer::run_seq;

		inline void operation(void);
};


class Sum : 
	public Consumer<FilteredDecimalVectorBlock*>
{
	typedef FilteredDecimalVectorBlock Data;
	typedef Data_Stream_struct<Data*> Input_data;

	private:
        Decimal *var_h;

	public:
		Sum(Decimal *_var_h,int threads);

		inline void operation(void);
};

#endif