#Add makefiles
export MAKE_DIR=../../..

include $(MAKE_DIR)/engine_conf.mk
include $(MAKE_DIR)/dbms_makefile.mk

ifndef HEPF_VERBOSE
export HEPF_VERBOSE = no
endif
ifndef HEPF_INTEL
export HEPF_INTEL = yes
endif
ifndef HEPF_ROOT
export HEPF_ROOT = no
endif
ifndef HEPF_SCHEDULER
export HEPF_SCHEDULER = yes
endif
ifndef HEPF_DEBUG
export HEPF_DEBUG = no
endif
ifndef HEPF_REUSE_EVENTS
export HEPF_REUSE_EVENTS = yes
endif
ifndef ALL_LOAD
export ALL_LOAD = yes
endif
ifndef DB_DIR
export DB_DIR = \"../../../data/la\"
endif
ifndef THREADS_PER_EVENT
export THREADS_PER_EVENT=2
endif

SHELL = /bin/sh

BIN_NAME = 6_HEPFrame

CXXFLAGS   = -DTHREAD_PER_EVENT=$(THREADS_PER_EVENT) -DDB_DIR=$(DB_DIR) \
				-DBSIZE=$(BSIZE) -DDATASET=$(DATASET) -DD_ROOT -DD_THREAD_BALANCE \
				-Wno-unused-but-set-parameter -Wno-missing-field-initializers -Wall \
				-Wextra -std=c++0x -Wno-comment -Wno-deprecated-declarations \
				-Wno-unused-parameter -Wno-sign-compare -lboost_thread -lboost_system \
				-lboost_chrono -DD_MULTICORE

ifeq ($(HEPF_INTEL),yes)
CXX        = icpc  -DD_HEPF_INTEL
LD         = icpc  -DD_HEPF_INTEL
else
CXX        = g++
LD         = g++
endif

#ROOTCFLAGS = $(shell root-config --cflags)
#ROOTLIBS   = $(shell root-config --glibs)
GPU_ROOTGLIBS= -L$(HEP_DIR)/lib -lHEPFrame --compiler-options " $(shell root-config --glibs) \
				-lMinuit -lHtml -lEG -lPhysics -lTreePlayer -lHEPFrame -lboost_program_options \
				-lboost_thread -lboost_system"
ROOTGLIBS  = $(shell root-config --glibs) -lMinuit -lEG -lPhysics -lTreePlayer
OTHER_LIBS = -lHEPFrame -lboost_program_options -lboost_thread -lboost_system -lEngine

ifeq ($(HEPF_INTEL),yes)
	OTHER_LIBS +=
endif

ifeq ($(HEPF_MPI),yes)
	ifeq ($(HEPF_INTEL),yes)
		CXX = mpiicpc -DD_HEPF_INTEL #-xMIC-AVX512
		LD  = mpiicpc -DD_HEPF_INTEL #-xMIC-AVX512
		CXXFLAGS += -DD_MPI
	else
		CXX = mpicxx
		LD  = mpicxx
		CXXFLAGS += -DD_MPI
	endif
endif


UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Darwin)
	CXXFLAGS+= -Wno-unused-command-line-argument
endif

ifeq ($(HEPF_THREAD_BALANCE),no)
	CXXFLAGS:=$(filter-out -DD_THREAD_BALANCE,$(CXXFLAGS))
endif

ifeq ($(HEPF_ROOT),no)
	CXXFLAGS:=$(filter-out -DD_ROOT,$(CXXFLAGS))
	ROOTGLIBS = -DD_NO_ROOT
	GPU_ROOTGLIBS = -DD_NO_ROOT
endif

ifeq ($(HEPF_SCHEDULER),yes)
	CXXFLAGS += -DD_SCHEDULER
endif

ifeq ($(HEPF_SEQ),yes)
	CXXFLAGS += -DD_SEQ
endif

ifeq ($(HEPF_DEBUG),yes)
	CXXFLAGS += -gdwarf-3
else
	CXXFLAGS += -O3
endif

ifeq ($(HEPF_REUSE_EVENTS),yes)
	CXXFLAGS += -DD_REUSE_EVENTS
endif

ifeq ($(ALL_LOAD),yes)
	CXXFLAGS += -DD_ALL_LOAD
endif

ifeq ($(HEPF_VERBOSE),yes)
	CXXFLAGS += -DD_VERBOSE
endif

HEP_DIR=../../lib

ifeq ($(strip $(BOOST_DIR)),)
	INCLUDES = -I$(HEP_DIR)/src -I$(ENGINE)/include
	LIBS = -L$(HEP_DIR)/lib -L$(LIB_DIR_ENGINE)
else
	INCLUDES = -I$(BOOST_DIR) -I$(HEP_DIR)/src -I$(ENGINE)/include
	LIBS = -L$(BOOST_DIR)/stage/lib -L$(HEP_DIR)/lib -L$(LIB_DIR_ENGINE)
endif

#ifeq ($(HEPF_GPU),yes)
#	ifeq ($(HEPF_MPI),yes)
#		CXX = nvcc $(INCLUDES) $(GPULIBS) -c -O3 -lcurand -ccbin=mpiicpc --compiler-options "
#		LD = nvcc $(INCLUDES) $(GPULIBS) -O3 -lcurand -ccbin=mpiicpc --compiler-options "
#		CXXFLAGS += -DD_GPU -DD_HEPF_INTEL -DD_MPI
#		LIBS += "
#		ROOTGLIBS = $(GPU_ROOTGLIBS)
#	else
#		CXX = nvcc $(INCLUDES) $(GPULIBS) -c -O3 -lcurand --compiler-options "
#		LD = nvcc $(INCLUDES) $(GPULIBS) -O3 -lcurand --compiler-options "
#		CXXFLAGS += -DD_GPU
#		LIBS += "
#		ROOTGLIBS = $(GPU_ROOTGLIBS)
#	endif
#endif

################################################################################
# Control awesome stuff
################################################################################

SRC_DIR = src
BIN_DIR = bin
BUILD_DIR = build
SRC = $(wildcard $(SRC_DIR)/*.cxx)
OBJ = $(patsubst src/%.cxx,build/%.o,$(SRC))
DEPS = $(patsubst build/%.o,build/%.d,$(OBJ))

vpath %.cxx $(SRC_DIR)

################################################################################
# Rules
################################################################################

.DEFAULT_GOAL = all

$(BUILD_DIR)/%.d: %.cxx
	$(CXX) -M $(CXXFLAGS) $(INCLUDES) $(LIBS) $< -o $@ $(OTHER_LIBS)

$(BUILD_DIR)/%.o: %.cxx
	$(CXX) -c $(CXXFLAGS) $(INCLUDES) $(LIBS) $< -o $@ $(OTHER_LIBS)

$(BIN_DIR)/$(BIN_NAME): $(OBJ) $(DEPS)
	$(CXX) $(CXXFLAGS) $(INCLUDES) $(LIBS) -o $@ $(OBJ) $(OTHER_LIBS)

checkdirs:
	@mkdir -p $(BUILD_DIR)
	@mkdir -p $(BIN_DIR)
	
all: dbms_make clean make_lib checkdirs $(BIN_DIR)/$(BIN_NAME)

dbms_make: $(MAKE_DIR)/dbms_makefile.mk
	make -f $(MAKE_DIR)/dbms_makefile.mk basic

make_lib:
	cd ../../lib && $(MAKE) all

clean:
	make -f $(MAKE_DIR)/dbms_makefile.mk clean
	rm -f $(BUILD_DIR)/*

clean_all:
	cd ../../lib && $(MAKE) clean
	rm -f $(BUILD_DIR)/* $(BIN_DIR)/$(BIN_NAME)

################################################################################
# Olters
################################################################################

REUSE_ALL_LOAD = $(BIN_NAME)_reuse_all
REMOVE_ALL_LOAD = $(BIN_NAME)_remove_all
REUSE = $(BIN_NAME)_reuse
REMOVE = $(BIN_NAME)_remove

all_v: reuse reuse_all remove remove_all

reuse:
	export ALL_LOAD=no && export HEPF_REUSE_EVENTS=yes && \
	$(MAKE) clean_all && $(MAKE) all && mv -f $(BIN_DIR)/$(BIN_NAME) $(BIN_DIR)/$(REUSE)

reuse_all:
	export ALL_LOAD=yes && export HEPF_REUSE_EVENTS=yes && \
	$(MAKE) clean_all && $(MAKE) all && mv -f $(BIN_DIR)/$(BIN_NAME) $(BIN_DIR)/$(REUSE_ALL_LOAD)

remove:
	export ALL_LOAD=no && export HEPF_REUSE_EVENTS=no && \
	$(MAKE) clean_all && $(MAKE) all && mv -f $(BIN_DIR)/$(BIN_NAME) $(BIN_DIR)/$(REMOVE)

remove_all:
	export ALL_LOAD=yes && export HEPF_REUSE_EVENTS=no && \
	$(MAKE) clean_all && $(MAKE) all && mv -f $(BIN_DIR)/$(BIN_NAME) $(BIN_DIR)/$(REMOVE_ALL_LOAD)