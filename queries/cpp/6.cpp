/*
 * Copyright (c) 2018 João Afonso. All rights reserved.
 *
 * This query was automatically generated
 */
#include <chrono>
#include <iostream>
#include <string>
#include <vector>
#include "include/block.hpp"
#include "include/database.hpp"
#include "include/dot.hpp"
#include "include/filter.hpp"
#include "include/fold.hpp"
#include "include/functions.hpp"
#include "include/krao.hpp"
#include "include/lift.hpp"
#include "include/matrix.hpp"
#include "include/types.hpp"

inline bool filter_var_a(std::vector<engine::Literal> args){ return args[0]>="1994-01-01"&&args[0]<"1995-01-01";}
inline bool filter_var_b(std::vector<engine::Decimal> args){ return args[0]>=0.05&&args[0]<=0.07;}
inline bool filter_var_d(std::vector<engine::Decimal> args){return args[0]<24;}
inline engine::Decimal lift_var_f(std::vector<engine::Decimal> args) {return args[0]*args[1];}

#ifndef DATASET
	#define DATASET "1"
#endif

using namespace std;
using namespace engine;

int main() {
  string tpch = string("TPCH_") + DATASET;

  auto start = std::chrono::high_resolution_clock::now();
  Database db("data/la",tpch,false);

  engine::Bitmap *var_lineitem_shipdate = new engine::Bitmap(db.data_path,db.database_name,"lineitem","shipdate");
  engine::DecimalVector *var_lineitem_discount = new engine::DecimalVector(db.data_path,db.database_name,"lineitem","discount");
  engine::DecimalVector *var_lineitem_quantity = new engine::DecimalVector(db.data_path,db.database_name,"lineitem","quantity");
  engine::DecimalVector *var_lineitem_extendedprice = new engine::DecimalVector(db.data_path,db.database_name,"lineitem","extendedprice");

  engine::FilteredBitVector *var_a_pred = new engine::FilteredBitVector(var_lineitem_shipdate->nLabelBlocks);
  engine::FilteredBitVector *var_a = new engine::FilteredBitVector(var_lineitem_shipdate->nBlocks);
  engine::FilteredBitVector *var_b = new engine::FilteredBitVector(var_lineitem_discount->nBlocks);
  engine::FilteredBitVector *var_c = new engine::FilteredBitVector(var_a->nBlocks);
  engine::FilteredBitVector *var_d = new engine::FilteredBitVector(var_lineitem_quantity->nBlocks);
  engine::FilteredBitVector *var_e = new engine::FilteredBitVector(var_c->nBlocks);
  engine::DecimalVector *var_f = new engine::DecimalVector(var_lineitem_extendedprice->nBlocks);
  engine::FilteredDecimalVector *var_g = new engine::FilteredDecimalVector(var_e->nBlocks);
  engine::Decimal *var_h = new engine::Decimal();

  //std::cout << "Load " << var_lineitem_shipdate->nLabelBlocks << " Label blocks" << std::endl;
  //std::cout << "Load " << var_lineitem_shipdate->nBlocks << " blocks" << std::endl;


  // Load and Filter l_shipdate_label
  #pragma omp parallel for
  for (engine::Size i = 0; i < var_lineitem_shipdate->nLabelBlocks; ++i) {
  	 // Load de dados em blocos - Otimização: Distribuição da leitura dos blocos por N* Threads
	 var_lineitem_shipdate->loadLabelBlock(i);
	 var_a_pred->blocks[i] = new engine::FilteredBitVectorBlock();

	 // Stream de dados é implementado por uma queue FIFO que guarda o valor dos produtores e permite que os consumidores obtenham os dados necessários

	 // Filtrar os dados lidos - Otimização: Distribuição da filtração pelas threads
	 filter(filter_var_a,{*(var_lineitem_shipdate->labels[i])},var_a_pred->blocks[i]); 	//Construção da matriz var_a_pred
	 var_lineitem_shipdate->deleteLabelBlock(i);									 	//Free
  }

  /*
   * A matriz var_a_pred tem de ser calculada na sua totalidade antes de começar a operação de Dot.
   * Esta dependência difere das restantes uma vez que a matriz não pode ser dividida em blocos ...
   */

  // Load l_shipdate and dot with l_shipdate_label ...
  #pragma omp parallel for
  for(engine::Size i = 0; i < var_lineitem_shipdate->nBlocks; ++i) {
	 var_lineitem_shipdate->loadBlock(i);
	 var_a->blocks[i] = new engine::FilteredBitVectorBlock();
	 // Dot 
	 dot(*var_a_pred, *(var_lineitem_shipdate->blocks[i]), var_a->blocks[i]);
	 var_lineitem_shipdate->deleteBlock(i);
	 //load discount
	 var_lineitem_discount->loadBlock(i);
	 var_b->blocks[i] = new engine::FilteredBitVectorBlock();
	 filter(filter_var_b,
		{
		  *(var_lineitem_discount->blocks[i])
		},
		var_b->blocks[i]);

	 var_c->blocks[i] = new engine::FilteredBitVectorBlock();
	 krao(*(var_a->blocks[i]),
	 	  *(var_b->blocks[i]),
	 	  var_c->blocks[i]
	 	 );
	 var_a->deleteBlock(i);
	 var_b->deleteBlock(i);

	 //Load quantity
	 var_lineitem_quantity->loadBlock(i);
	 var_d->blocks[i] = new engine::FilteredBitVectorBlock();
	 filter(filter_var_d,
		{
		  *(var_lineitem_quantity->blocks[i])
		},
		var_d->blocks[i]);
	 var_lineitem_quantity->deleteBlock(i);

	 var_e->blocks[i] = new engine::FilteredBitVectorBlock();
	 krao(*(var_c->blocks[i]), *(var_d->blocks[i]), var_e->blocks[i]);
	 var_c->deleteBlock(i);
	 var_d->deleteBlock(i);

	 //Load extendedprice
	 var_lineitem_extendedprice->loadBlock(i);
	 var_f->blocks[i] = new engine::DecimalVectorBlock();
	 lift(lift_var_f,
		{
		  *(var_lineitem_extendedprice->blocks[i]),
		  *(var_lineitem_discount->blocks[i])											//Reutilização do discount
		},
		var_f->blocks[i]);
	 var_lineitem_extendedprice->deleteBlock(i);
	 var_lineitem_discount->deleteBlock(i);

	 var_g->blocks[i] = new engine::FilteredDecimalVectorBlock();
	 krao(*(var_e->blocks[i]), *(var_f->blocks[i]), var_g->blocks[i]);
	 var_e->deleteBlock(i);
	 var_f->deleteBlock(i);

	 sum(*var_g->blocks[i], var_h);
	 var_g->deleteBlock(i);
  }

  delete var_a_pred;

  std::cout << (*var_h) << std::endl;

  delete var_h;

  auto end = std::chrono::high_resolution_clock::now();
  std::cout
	 //<< "Completed in "
	 << std::chrono::duration_cast<std::chrono::nanoseconds>(end-start).count()
	 //<< " ns"
	 << std::endl;

  return 0;
}
