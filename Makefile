all:
	mkdir -p $(HET2_DEPLOY)/lib/run/actor_pipes
	python setup.py install --prefix=$(HET2_DEPLOY)
	$(MAKE) html --directory=doc

clean:
	python setup.py clean

.PHONY: test
test:
	py.test

install:
	python setup.py install --prefix=$(HET2_DEPLOY)
	cp actors.conf.example $(HET2_DEPLOY)/etc

uninstall:
	rm -rf $(HET2_DEPLOY)/lib/python$(PYTHON_VERSION)/site-packages/pycommon
	rm -rf $(HET2_DEPLOY)/lib/python$(PYTHON_VERSION)/site-packages/PyCommon*

.PHONY: debug
debug: all

install_docs:
	mkdir -p ~/GeneratedDocs/pycommon
	cp -r doc/_build/html/* ~/GeneratedDocs/pycommon
