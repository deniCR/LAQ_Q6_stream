CLASS=test

include engine_conf.mk

HEPF_VERBOSE = yes
HEPF_INTEL = yes
HEPF_ROOT = no
HEPF_SCHEDULER = yes
HEPF_DEBUG = no
HEPF_REUSE_EVENTS = yes
ALL_LOAD = yes

SHELL = /bin/sh
HEP_DIR = HEPFrame/lib

BIN_NAME = 6_HEPFrame

CXXFLAGS   = -DBSIZE=$(BSIZE) -DDATASET=$(DATASET) -DD_ROOT -DD_THREAD_BALANCE -Wno-unused-but-set-parameter -Wno-missing-field-initializers -Wall -Wextra -std=c++0x -Wno-comment -Wno-deprecated-declarations -Wno-unused-parameter -Wno-sign-compare -lboost_thread -lboost_system -lboost_chrono -DD_MULTICORE

ifeq ($(HEPF_INTEL),yes)
CXX        = icpc  -DD_HEPF_INTEL
LD         = icpc  -DD_HEPF_INTEL
else
CXX        = g++
LD         = g++
endif

ROOTCFLAGS = $(shell root-config --cflags)
ROOTLIBS   = $(shell root-config --glibs)
GPU_ROOTGLIBS= -L$(HEP_DIR)/lib -lHEPFrame --compiler-options " $(shell root-config --glibs) -lMinuit -lHtml -lEG -lPhysics -lTreePlayer -lHEPFrame -lboost_program_options -lboost_thread -lboost_system"# -lmkl_blas95_lp64 -lmkl_lapack95_lp64 -lmkl_intel_lp64 -lmkl_sequential -lmkl_core -lboost_thread -lboost_system"
ROOTGLIBS  = $(shell root-config --glibs) -lMinuit -lEG -lPhysics -lTreePlayer
OTHER_LIBS_H = -lHEPFrame -lboost_program_options -lboost_thread -lboost_system -lEngine

ifeq ($(HEPF_INTEL),yes)
	OTHER_LIBS_H +=
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

ifeq ($(HEPF_VERBOSE),yes)
	CXXFLAGS += -DD_VERBOSE
endif

ifeq ($(ALL_LOAD),yes)
	CXXFLAGS += -DD_ALL_LOAD
endif

ifeq ($(strip $(BOOST_DIR)),)

INCLUDES_H = -I$(HEP_DIR)/src -I$(ENGINE)/include

LIBS_H = -L$(HEP_DIR)/lib -L$(LIB_DIR_ENGINE)

else

INCLUDES_H = -I$(BOOST_DIR)/include -I$(HEP_DIR)/src -I$(ENGINE)/include

LIBS_H = -L$(BOOST_DIR)/lib -L$(HEP_DIR)/lib -L$(LIB_DIR_ENGINE)

endif

ifeq ($(HEPF_GPU),yes)
	ifeq ($(HEPF_MPI),yes)
		CXX = nvcc $(INCLUDES_H) $(GPULIBS) -c -O3 -lcurand -ccbin=mpiicpc --compiler-options "
		LD = nvcc $(INCLUDES_H) $(GPULIBS) -O3 -lcurand -ccbin=mpiicpc --compiler-options "
		CXXFLAGS += -DD_GPU -DD_HEPF_INTEL -DD_MPI
		LIBS_H += "
		ROOTGLIBS = $(GPU_ROOTGLIBS)
	else
		CXX = nvcc $(INCLUDES_H) $(GPULIBS) -c -O3 -lcurand --compiler-options "
		LD = nvcc $(INCLUDES_H) $(GPULIBS) -O3 -lcurand --compiler-options "
		CXXFLAGS += -DD_GPU
		LIBS_H += "
		ROOTGLIBS = $(GPU_ROOTGLIBS)
	endif
endif

################################################################################
# Control awesome stuff
################################################################################

QUERY_DIR = HEPFrame/Analysis/q6
SRC_DIR = $(QUERY_DIR)/src
BIN_DIR = hepf/bin
BUILD_DIR = $(QUERY_DIR)/build
SRC = $(wildcard $(SRC_DIR)/*.cxx)
OBJ = $(patsubst src/%.cxx,build/%.o,$(SRC))
DEPS = $(patsubst build/%.o,build/%.d,$(OBJ))

vpath %.cxx $(SRC_DIR)

################################################################################
# Rules
################################################################################

.DEFAULT_GOAL = all

$(BUILD_DIR)/%.d: %.cxx
	$(CXX) -M $(CXXFLAGS) $(INCLUDES_H) $(LIBS_H) $< -o $@ $(OTHER_LIBS_H)

$(BUILD_DIR)/%.o: %.cxx
	$(CXX) -c $(CXXFLAGS) $(INCLUDES_H) $(LIBS_H) $< -o $@ $(OTHER_LIBS_H)

$(BIN_DIR)/$(BIN_NAME): $(OBJ) $(DEPS)
	$(CXX) $(CXXFLAGS) $(INCLUDES_H) $(LIBS_H) -o $@ $(OBJ) $(OTHER_LIBS_H)

checkdirs:
	@mkdir -p $(BUILD_DIR)
	@mkdir -p $(BIN_DIR)
	
all: checkdirs $(BIN_DIR)/$(BIN_NAME)

clean:
	rm -f $(BUILD_DIR)/* $(BIN_DIR)/$(BIN_NAME)