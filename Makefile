all:
	python setup.py install --prefix=$(HET2_DEPLOY)

clean:
	python setup.py clean

test:
	py.test

install:
	python setup.py install --prefix=$(HET2_DEPLOY)

uninstall:
	rm -rf $(HET2_DEPLOY)/lib/python$(PYTHON_VERSION)/site-packages/pycommon
	rm -rf $(HET2_DEPLOY)/lib/python$(PYTHON_VERSION)/site-packages/PyCommon*

.PHONY: debug
debug: all