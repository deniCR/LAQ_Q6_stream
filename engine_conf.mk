ifndef MAKE_DIR
MAKE_DIR=.
endif

CXX = icpc

ifndef DATA_PATH
export DATA_PATH = $(MAKE_DIR)/data/la
endif
ifndef DBGEN_DATA_PATH
export DBGEN_DATA_PATH = $(MAKE_DIR)/data/dbgen
endif
ifndef OMP_NUM_THREADS
export OMP_NUM_THREADS=48
endif
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
ifndef BSIZE
export BSIZE=65536
endif
ifndef DATASET
export DATASET=32
endif
ifndef BUFFER_SIZE
export BUFFER_SIZE=65536
endif
ifndef PAPI_FLAG
export PAPI_FLAG=1
endif

################################################################################
# Dirs
################################################################################

BOOST_DIR = /home/a75655/boost_1_69_0
BOOST_LIB = /usr/lib/x86_64-linux-gnu
IIT_DIR = /share/apps/intel/vtune_amplifier_2019/include
IIT_LIB = /share/apps/intel/vtune_amplifier_2019/lib64
PAPI_INC = /share/apps/papi/5.5.0/include
PAPI_LIB = /share/apps/papi/5.5.0/lib

ENGINE = $(MAKE_DIR)/engine
ENGINE_INCLUDE = -L$(MAKE_DIR)/lib -I$(MAKE_DIR)/engine/include