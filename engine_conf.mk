ifndef MAKE_DIR
MAKE_DIR=.
endif

CXX = dpcpp -std=c++17

ifndef DATA_DIR
export DATA_DIR = $(MAKE_DIR)/data
endif
ifndef DATA_PATH
export DATA_PATH = $(MAKE_DIR)/data/la
endif
ifndef DBGEN_DATA_PATH
export DBGEN_DATA_PATH = $(MAKE_DIR)/data/dbgen
endif
# Default OpenMP threads
ifndef OMP_NUM_THREADS
export OMP_NUM_THREADS=2
endif
# Default Stream threads
ifndef READ_THREADS
export READ_THREADS=1
endif
ifndef WORK_THREADS
export WORK_THREADS=2
endif
ifndef DOT_THREADS
export DOT_THREADS=2
endif
ifndef HAD_THREADS
export HAD_THREADS=2
endif
# Default Block size
ifndef BSIZE
export BSIZE=65536
endif
# Default TCP-H dataset size
ifndef DATASET
export DATASET=1
endif

################################################################################
# "Global" Dirs
################################################################################

BOOST_DIR = /usr/include
BOOST_LIB = /usr/lib/x86_64-linux-gnu
IIT_DIR = /share/apps/intel/vtune_amplifier_2019/include
IIT_LIB = /share/apps/intel/vtune_amplifier_2019/lib64
#PAPI = /share/apps/papi/5.5.0/include
PAPI = /usr/local/include
#PAPI_LIB = /share/apps/papi/5.5.0/lib
PAPI_LIB = /usr/local/lib

ENGINE = $(MAKE_DIR)/engine
ENGINE_INCLUDE = -L$(MAKE_DIR)/lib -I$(MAKE_DIR)/engine/include
