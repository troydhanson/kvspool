all: kvspool

kvspool: kvspool.c ../src/libkvspool.a
	rm -rf build 
	python build.py build

install:
	@echo "Installing (typically in /usr/local/lib/python2.6/dist-packages/)"
	python build.py install

.PHONY: clean distclean
clean:
	@python build.py $@
	rm -rf build

distclean: clean
