all:
	python setup.py install --prefix=$(HET2_DEPLOY)
	$(MAKE) html --directory=doc

clean:
	python setup.py clean

.PHONY: test
test:
	py.test

install:
	python setup.py install --prefix=$(HET2_DEPLOY)

uninstall:
	rm -rf $(HET2_DEPLOY)/lib/python$(PYTHON_VERSION)/site-packages/pycommon
	rm -rf $(HET2_DEPLOY)/lib/python$(PYTHON_VERSION)/site-packages/PyCommon*

.PHONY: debug
debug: all

install_docs:
	cp doc/_build/html ~/GeneratedDocs/pycommon
