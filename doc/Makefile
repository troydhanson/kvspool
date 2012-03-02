all: kvspool.html

kvspool.html: kvspool.txt
	asciidoc -a toc2 $<

.PHONY: clean

clean:
	rm -f kvspool.html
