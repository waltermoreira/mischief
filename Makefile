all:
	mkdir -p $(HET2_DEPLOY)/lib/run/actor_pipes
	python setup.py install --prefix=$(HET2_DEPLOY)
	$(MAKE) html --directory=doc

clean:
	python setup.py clean

.PHONY: test
test:
	mkdir -p $(HET2_DEPLOY)/test/pycommon_tests
	cp -a test $(HET2_DEPLOY)/test/pycommon_tests
	py.test $(HET2_DEPLOY)/test/pycommon_tests

install:
	mkdir -p $(HET2_DEPLOY)/lib/run/actor_pipes
	python setup.py install --prefix=$(HET2_DEPLOY)
	cp py_logging.conf.example $(HET2_DEPLOY)/etc

uninstall:
	rm -rf $(HET2_DEPLOY)/lib/python$(PYTHON_VERSION)/site-packages/pycommon
	rm -rf $(HET2_DEPLOY)/lib/python$(PYTHON_VERSION)/site-packages/PyCommon*

.PHONY: debug
debug: all

install_docs:
	mkdir -p $(HET2_DOCS)/pycommon
	cp -r doc/_build/html/* $(HET2_DOCS)/pycommon
