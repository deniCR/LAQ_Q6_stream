################################################################################
# Makefile for ENGINE
################################################################################
ifndef MAKE_DIR
MAKE_DIR=.
endif

-include $(MAKE_DIR)/engine_conf.mk

################################################################################
# Dirs 
################################################################################

LAQ_DIR = $(MAKE_DIR)/laq_driver
ENGINE = $(MAKE_DIR)/engine
LINTER = $(MAKE_DIR)/lib/styleguide/cpplint/cpplint.py
GTEST_DIR = $(MAKE_DIR)/lib/googletest/googletest/include
CEREAL = $(MAKE_DIR)/lib/cereal/include
DATA_DIR = $(MAKE_DIR)/data
QUERIES_DIR = $(MAKE_DIR)/queries/cpp
ENGINE_OBJ_DIR = $(ENGINE)/build
ENGINE_SRC_DIR = $(ENGINE)/src

LIB_NAME = libEngine
LIB_DIR_ENGINE = $(MAKE_DIR)/lib
ENGINE_LIB = $(LIB_DIR_ENGINE)/$(LIB_NAME)

################################################################################
# Compiler Flags & Libraries
################################################################################

CXX_ENGINE = icpc
#-Werror
#/usr/lib/llvm-6.0/bin/clang++

CXXFLAGS_ENGINE = -O3 -std=c++11 -Wall -Wextra -Wshadow -Wconversion -pedantic \
					-qopenmp -DBSIZE=$(BSIZE) -DDATASET=$(DATASET)

INCLUDE_E = $(ENGINE_INCLUDE) -L$(BOOST_DIR)/stage/lib -I$(BOOST_DIR)
LIBS_E = -lEngine

ifeq ($(PAPI_TEST),yes)
	CXXFLAGS_ENGINE += -DD_PAPI
	C_PAPI = include
endif

ifeq ($(PAPI_OPS_TEST),yes)
	CXXFLAGS_ENGINE += -DD_PAPI_OPS
	C_PAPI = include
endif

ifeq ($(C_PAPI),include)
	INCLUDE_E += -L$(PAPI_LIB) -I$(PAPI_INC)
	LIBS_E += -lpapi
endif

ifeq ($(VTUNE_TEST),yes)
	CXXFLAGS_ENGINE += -DD_VTUNE
	INCLUDE_E += -L$(IIT_LIB) -I$(IIT_DIR)
	LIBS_E += -littnotify
endif

ifeq ($(LOAD_TIMES),yes)
	CXXFLAGS_ENGINE += -DD_LOAD_TIMES
endif
ifeq ($(JUST_TIME),yes)
	CXXFLAGS_ENGINE:=$(filter-out -DD_PAPI,$(CXXFLAGS_ENGINE))
	CXXFLAGS_ENGINE:=$(filter-out -DD_PAPI_OPS,$(CXXFLAGS_ENGINE))
	CXXFLAGS_ENGINE:=$(filter-out -DD_VTUNE,$(CXXFLAGS_ENGINE))
	CXXFLAGS_ENGINE:=$(filter-out -DD_LOAD_TIMES,$(CXXFLAGS_ENGINE))

	INCLUDE_E:=$(filter-out -L$(PAPI_LIB),$(INCLUDE_E))
	INCLUDE_E:=$(filter-out -I$(PAPI_INC),$(INCLUDE_E))
	INCLUDE_E:=$(filter-out -L$(IIT_LIB),$(INCLUDE_E))
	INCLUDE_E:=$(filter-out -I$(IIT_DIR),$(INCLUDE_E))

	LIBS_E:=$(filter-out -littnotify,$(LIBS_E))
	LIBS_E:=$(filter-out -lpapi,$(LIBS_E))
endif

################################################################################
# Engine queries
################################################################################

ENGINE_OBJ = $(ENGINE_OBJ_DIR)/block.o \
			 $(ENGINE_OBJ_DIR)/dot.o \
			 $(ENGINE_OBJ_DIR)/filter.o \
			 $(ENGINE_OBJ_DIR)/filter_old.o \
			 $(ENGINE_OBJ_DIR)/fold.o \
			 $(ENGINE_OBJ_DIR)/fold_old.o \
			 $(ENGINE_OBJ_DIR)/functions.o \
			 $(ENGINE_OBJ_DIR)/krao.o \
			 $(ENGINE_OBJ_DIR)/krao_old.o \
			 $(ENGINE_OBJ_DIR)/lift.o \
			 $(ENGINE_OBJ_DIR)/lift_old.o \
			 $(ENGINE_OBJ_DIR)/matrix.o \
			 $(ENGINE_OBJ_DIR)/database.o

.PHONY: all basic engine clean delete linter test count

all: basic

basic: $(ENGINE_LIB).a \
		$(ENGINE)/bin/load

engine: basic \
		$(ENGINE)/bin/6_reuse \
		$(ENGINE)/bin/6_reuse_time \
		$(ENGINE)/bin/6_remove \
		$(ENGINE)/bin/6_remove_nvec \
		$(ENGINE)/bin/6_reuse_PAPI

papi: basic \
		$(ENGINE)/bin/6_reuse_PAPI \
		$(ENGINE)/bin/6_reuse_PAPI_2 \
		$(ENGINE)/bin/6_remove \
		$(ENGINE)/bin/6_remove_2 \
		$(ENGINE)/bin/6_remove_nvec \
		$(ENGINE)/bin/6_remove_nvec_2

time: basic \
		$(ENGINE)/bin/6_reuse_time \
		$(ENGINE)/bin/14_reuse

########## Engine lib ##########

$(ENGINE_LIB).a: $(ENGINE)/build $(ENGINE_OBJ)
	ar -r $@ $(ENGINE_OBJ)

########## Engine objects ##########

$(ENGINE_OBJ_DIR)/%.o: $(ENGINE_SRC_DIR)/%.cpp $(ENGINE)/include/%.hpp
	$(CXX_ENGINE) $(CXXFLAGS_ENGINE) -c $< -I$(ENGINE) -I$(CEREAL) -o $@

$(ENGINE)/bin/%: $(QUERIES_DIR)/%.cpp
	$(CXX_ENGINE) $(CXXFLAGS_ENGINE) $(INCLUDE_E) -o $@ $^ $(LIBS_E)

$(ENGINE)/bin/load: $(DATA_DIR)/tpch_createDB.cpp
	$(CXX_ENGINE) $(CXXFLAGS_ENGINE) $(INCLUDE_E) -o $@ $^ $(LIBS_E)

########## Needed directories ##########

$(ENGINE)/build:
	mkdir -p $@ $(ENGINE)/bin

################################################################################
# Others
################################################################################

test:
	$(LAQ_DIR)/bin/test_laq

count:
	cloc engine laq_driver sql_driver data queries makefile

clean:
	rm -fr $(LAQ_DIR)/build $(ENGINE)/build
	rm -fr $(LAQ_DIR)/bin $(ENGINE)/bin
	rm -f $(LIB_DIR_ENGINE)/$(LIB_NAME)

delete: clean
	rm -fr  $(DATA_DIR)/la/*

deleteAll: clean delete
	cd $(DATA_DIR)/dbgen/
	rm -r -f 1/ 2/ 4/ 8/ 16/ 32/ 64/

%: $(ENGINE)/bin/%
	$(ENGINE)/bin/$@

################################################################################
# LAQ
################################################################################

laq: $(LAQ_DIR)/build $(LAQ_DIR)/bin/test_laq

linter: $(LINTER)
	@$< --extensions=hpp,cpp \
		$(LAQ_DIR)/*/*.cpp \
		$(LAQ_DIR)/*/*.hpp \
		$(ENGINE)/*/*.cpp \
		$(ENGINE)/*/*.hpp \
		$(MAKE_DIR)/queries/cpp/*.cpp \
		$(DATA_DIR)/*.cpp

$(LAQ_DIR)/bin/test_laq: $(LAQ_DIR)/test/test_laq.cpp \
						 $(LAQ_DIR)/build/laq_parser.o \
						 $(LAQ_DIR)/build/lex.yy.o \
						 $(LAQ_DIR)/build/laq_driver.o \
						 $(LAQ_DIR)/build/parsing_tree.o \
						 $(LAQ_DIR)/build/laq_statement.o \
						 $(ENGINE_OBJ) \
						 libgtest.a
	$(CXX_ENGINE) $(CXXFLAGS_ENGINE) -isystem $(GTEST_DIR) -pthread $^ -o $@ -I $(LAQ_DIR) -I $(ENGINE)

########## LAQ parser objects ##########

$(LAQ_DIR)/build/laq_parser.o: $(LAQ_DIR)/build/laq_parser.cpp
	$(CXX_ENGINE) $(CXXFLAGS_ENGINE) -c $< -I $(LAQ_DIR) -I $(ENGINE) -I $(LAQ_DIR)/build -o $@

$(LAQ_DIR)/build/lex.yy.o: $(LAQ_DIR)/build/lex.yy.cpp
	$(CXX_ENGINE) $(CXXFLAGS_ENGINE) -c $< -I $(LAQ_DIR) -I $(ENGINE) -o $@

$(LAQ_DIR)/build/laq_driver.o: $(LAQ_DIR)/src/laq_driver.cpp
	$(CXX_ENGINE) $(CXXFLAGS_ENGINE) -c $< -I $(LAQ_DIR) -I $(ENGINE) -o $@

$(LAQ_DIR)/build/parsing_tree.o: $(LAQ_DIR)/src/parsing_tree.cpp
	$(CXX_ENGINE) $(CXXFLAGS_ENGINE) -c $< -I $(LAQ_DIR) -I $(ENGINE) -o $@

$(LAQ_DIR)/build/laq_statement.o: $(LAQ_DIR)/src/laq_statement.cpp
	$(CXX_ENGINE) $(CXXFLAGS_ENGINE) -c $< -I $(LAQ_DIR) -I $(ENGINE) -o $@


########## LAQ parser flex and bison ##########

$(LAQ_DIR)/build/laq_parser.cpp: $(LAQ_DIR)/src/laq_parser.yy
	bison -o $@ -d $<

$(LAQ_DIR)/build/lex.yy.cpp: $(LAQ_DIR)/src/laq_scanner.ll
	flex -o $@ $<

########## Needed directories ##########

$(LAQ_DIR)/build:
	mkdir -p $@ $(LAQ_DIR)/bin