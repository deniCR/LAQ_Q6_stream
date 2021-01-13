################################################################################
# Makefile for Stream
################################################################################

include engine_conf.mk
include dbms_makefile.mk

################################################################################
# Stream reletad Dirs 
################################################################################

#Some Dirs are defined in engine_conf.mk and dbms_makefile.mk
STREAM_DIR = stream
STREAM_INC = $(STREAM_DIR)/include
CHANNEL_DIR = Channel
STREAM_QUERIES = $(STREAM_DIR)/queries

################################################################################
# Compiler Flags & Libraries
################################################################################

CXX = dpcpp -std=c++17
#CXX = g++

#To change the variables THREADS, BSIZE and DATASET go to makefile engine_conf.mk
PRE_CONF = -DREAD_THREADS=$(READ_THREADS) -DWORK_THREADS=$(WORK_THREADS) \
			-DDOT_THREADS=$(DOT_THREADS) -DHAD_THREADS=$(HAD_THREADS) \
			-DBSIZE=$(BSIZE) -DDATASET=$(DATASET)

CXX_TMP_FLAGS = -O3 -pthread -qopenmp -Wall -Wextra -Wshadow -Wconversion -pedantic $(PRE_CONF)
#-qopt-report=4 -qopt-report-phase ipo -Wall -Wextra -Wshadow -Wconversion -pedantic

INCLUDE = -I$(STREAM_DIR) -L$(BOOST_DIR)/stage/lib -I$(BOOST_DIR) $(ENGINE_INCLUDE)
LIBS = -lboost_system -lboost_thread -lboost_chrono -lEngine

ifeq ($(PAPI_TEST),yes)
	CXX_TMP_FLAGS += -DD_PAPI
	C_PAPI = include
endif

ifeq ($(PAPI_OPS_TEST),yes)
	CXX_TMP_FLAGS += -DD_PAPI_OPS
	C_PAPI = include
endif

ifeq ($(C_PAPI),include)
	INCLUDE += -L$(PAPI_LIB) -I$(PAPI_INC)
	LIBS += -lpapi
endif

ifeq ($(VTUNE_TEST),yes)
	CXX_TMP_FLAGS += -DD_VTUNE
	INCLUDE += -L$(IIT_LIB) -I$(IIT_DIR)
	LIBS += -littnotify
endif

ifeq ($(LOAD_TIMES),yes)
	CXX_TMP_FLAGS += -DD_LOAD_TIMES
endif

ifeq ($(JUST_TIME),yes)
	CXX_TMP_FLAGS:=$(filter-out -DD_PAPI,$(CXX_TMP_FLAGS))
	CXX_TMP_FLAGS:=$(filter-out -DD_PAPI_OPS,$(CXX_TMP_FLAGS))
	CXX_TMP_FLAGS:=$(filter-out -DD_VTUNE,$(CXX_TMP_FLAGS))
	CXX_TMP_FLAGS:=$(filter-out -DD_LOAD_TIMES,$(CXX_TMP_FLAGS))
endif

################################################################################
# ...
################################################################################

DEPS = $(STREAM_INC)/consumer.hpp $(STREAM_INC)/consumer_producer.hpp \
		$(STREAM_INC)/producer.hpp $(STREAM_INC)/join.hpp

.PHONY: all clean stream dbms_basic dbms_all hepf

all: dbms_all stream papi time

dbms_basic:
	make -f dbms_makefile.mk basic

dbms_all:
	make -f dbms_makefile.mk all

hepf:
	rm -f hepf/bin/*
	export DB_DIR=\\\"data/la\\\" && \
	cd HEPFrame/Analysis/q6 && $(MAKE) clean_all && $(MAKE) all_v
	cp HEPFrame/Analysis/q6/bin/* hepf/bin/
	
stream: dbms_basic \
		$(STREAM_DIR)/bin/6_stream_reuse_time \
		$(STREAM_DIR)/bin/6_stream \
		$(STREAM_DIR)/bin/6_stream_remove \
		$(STREAM_DIR)/bin/6_stream_remove_nvec \
		$(STREAM_DIR)/bin/6_stream_reuse
		
papi: dbms_basic \
		$(STREAM_DIR)/bin/6_stream_remove \
		$(STREAM_DIR)/bin/6_stream_remove_2 \
		$(STREAM_DIR)/bin/6_stream_remove_nvec \
		$(STREAM_DIR)/bin/6_stream_remove_nvec_2 \
		$(STREAM_DIR)/bin/6_stream_reuse \
		$(STREAM_DIR)/bin/6_stream_reuse_2

time: dbms_basic \
		$(STREAM_DIR)/bin/6_stream_reuse_time \
		$(STREAM_DIR)/bin/14_stream_reuse

clean:
	rm -f $(STREAM_DIR)/bin/*
	make -f dbms_makefile.mk clean

$(STREAM_DIR)/bin:
	mkdir -p $@

$(STREAM_DIR)/bin/%: $(STREAM_QUERIES)/%.cpp
	$(CXX) $(CXX_TMP_FLAGS) $(INCLUDE) -o $@ $^ $(LIBS)

################################################################################
# Other
################################################################################

load: dbms_make $(ENGINE)/bin/load
	./$(ENGINE)/bin/load

%: $(STREAM_DIR)/bin/%
	$(STREAM_DIR)/bin/$@
