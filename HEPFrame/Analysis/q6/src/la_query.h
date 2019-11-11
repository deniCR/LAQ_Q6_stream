/*
 * Copyright (c) 2019 Daniel Rodrigues. All rights reserved.
 */
#ifndef la_query_h
#define la_query_h

#include <cstdlib>
#include <iostream>
#include <string>
#include <vector>
#include <algorithm>
#include "engine.hpp" //Engine library inclusion
#include <DataAnalysis.h>

#ifdef D_MPI
	#include <mpi.h>
#endif

using namespace std;


class la_query : public DataAnalysis 
{

	void recordVariables (unsigned cut_number, long unsigned this_event_counter);
	void recordVariablesBoost (long unsigned event);
	void writeVariables (void);
	void initRecord (unsigned cuts);
	void writeResults (void);
	void initialize (void);

	void fillHistograms (string cut_name);
	void finalize (void);

public:
	la_query (unsigned ncuts, unsigned num_events);
};

#endif
