ifndef HET2_AUXIL
	$(error HET2_AUXIL must point to the auxil directory)
endif

SWIG=$(HET2_AUXIL)/swig/bin/swig
PYTHON=$(HET2_AUXIL)/Python/bin/python
COMMON_DIR=$(HET2_WKSP)/common

all: het2_common/time/het2_time.py het2_common/time/het2_time_wrap.cxx
	$(PYTHON) setup.py build

het2_common/time/het2_time.py het2_common/time/het2_time_wrap.cxx: het2_common/time/het2_time.i
	(cd het2_common/time; $(SWIG) -python -c++ -I$(COMMON_DIR)/include het2_time.i)

install: all
	$(PYTHON) setup.py install --prefix=$(HET2_DEPLOY)
	mkdir -p $(HET2_DEPLOY)/test/trajectory_tests
	cp het2_common/trajectories/tolerances.json.example $(HET2_DEPLOY)/test/trajectory_tests/

debug: all

clean:
	$(PYTHON) setup.py clean
	rm -rf build
	rm -rf het2_common/time/het2_time_wrap.*
	rm -rf *.pyc
	rm -rf het2_common/time/.so
	rm -rf het2_common/time/het2_time.py
