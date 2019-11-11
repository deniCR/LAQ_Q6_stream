#include "la_query.h"

#define THIS_THREAD(x) thread_id * number_of_cuts + x

using namespace std;

extern boost::lockfree::queue<DataReader*> root_reader;
extern boost::lockfree::queue<DataReader*> root_read;
extern HEPEvent *events;
extern long unsigned event_counter;
extern unsigned num_threads;
extern unsigned thread_id;
boost::mutex *record_vars;


// Write here the variables and expressions to record per cut
#ifdef RecordVariables

#endif
// ***********************************************




// ***********************************************
// Method to record variables, do not edit
void la_query::initRecord (unsigned cuts) {

}

void la_query::recordVariables (unsigned cut_number, long unsigned this_event_counter) {

}

void la_query::recordVariablesBoost (long unsigned event) {

}

void la_query::writeVariables (void) {

}

// end

void la_query::writeResults (void) {
	/*
	#ifdef D_VERBOSE
	cout << "Writing events" << endl << endl;
	long unsigned last_n = 0;
	progressBar(0.0);
	#endif

	DataReader *rd;
	root_read.pop(rd);

	TFile *out_file = new TFile (output_root_file.c_str(), "recreate");
	TTree *new_to_write = rd->getfChain()->CloneTree(0);

	long unsigned number_events_cuts_passed = cuts_passed.size();

	for (long unsigned i = 0; i < number_events_cuts_passed; i++) {
		#ifdef D_VERBOSE
		if (i == (number_events_cuts_passed - 1)) {
			progressBar(1);
			cout << endl << endl << "All events wrote" << endl << endl;
		} else if (i >= last_n + 2000) {
			float n = (float)i/(float)number_events_cuts_passed;
			last_n = i;
			progressBar(n);
		}
		#endif

		events[cuts_passed[i]].loadEvent(cuts_passed[i]);
		events[cuts_passed[i]].write(new_to_write);
	}

	out_file->Write();
	out_file->Close();
	*/
}

