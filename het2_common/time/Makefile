COMMON_DIR=$(HET2_WKSP)/common

het2_time: het2_time.i $(COMMON_DIR)/include/HET2_Time.h $(COMMON_DIR)/src/HET2_Time.cpp
	swig -python -c++ -I$(COMMON_DIR)/include het2_time.i
	g++ -shared -fPIC het2_time_wrap.cxx $(COMMON_DIR)/src/HET2_Time.cpp -I$(COMMON_DIR)/include -I$(HET2_AUXIL)/Python/include/python$(PYTHON_VERSION) -I$(HET2_LIBS) -L$(HET2_AUXIL)/Python/lib -L$(HET2_LIBS)/libtime -ltime -lm -lrt -o _het2_time.so

all: het2_time
