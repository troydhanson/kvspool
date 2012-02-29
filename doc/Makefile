all: kvspool.html

kvspool.html: kvspool.txt
	#asciidoc -a theme=flask $<
	asciidoc $<

.PHONY: clean

clean:
	rm -f kvspool.html
