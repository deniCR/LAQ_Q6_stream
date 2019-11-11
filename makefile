# Deprecated Makefile
include dbms_makefile

CXX = g++

ifndef OMP_NUM_THREADS
export OMP_NUM_THREADS = 8
endif

ifndef DBSIZE
export DBSIZE = 32768
endif

CXX_TMP_FLAGS = -g -O2 -Wall -Wextra -Wshadow -Wconversion -pedantic -pthread\
				-fopenmp

ifdef BSIZE
CXXFLAGS = $(CXX_TMP_FLAGS) -DBSIZE=$(BSIZE)
else
CXXFLAGS = $(CXX_TMP_FLAGS)
endif

ifdef DATASET
CXXFLAGS = $(CXX_TMP_FLAGS) -DDATASET=$(DATASET)
else
CXXFLAGS = $(CXX_TMP_FLAGS)
endif

STREAM_DIR = stream
QUERIES_DIR = queries/cpp

.PHONY: all clean stream dbms_make

all: stream

stream: $(STREAM_DIR)/bin \
		$(STREAM_DIR)/bin/q6_stream 

clean_stream:
	rm -fr $(STREAM_DIR)/bin
	make -f dbms_makefile clean

$(STREAM_DIR)/bin/q6_stream: $(QUERIES_DIR)/6_stream.cpp \
							 $(STREAM_OBJ) $(ENGINE_OBJ) $(STREAM_DIR)/include/*
	$(CXX) $(CXXFLAGS) -I $(STREAM_DIR) -o $@ $^ -lboost_system -lboost_thread

$(STREAM_DIR)/bin:
	mkdir -p $@ $(STREAM_DIR)/bin

########## Other ##########

load: $(ENGINE_DIR)/bin/load
	./$(ENGINE_DIR)/bin/load

q6: $(ENGINE_DIR)/bin/q6
	./$(ENGINE_DIR)/bin/q6

q6_stream: $(STREAM_DIR)/bin/q6_stream
	./$(STREAM_DIR)/bin/q6_stream	
