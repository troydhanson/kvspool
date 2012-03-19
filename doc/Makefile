all: reader-writer.png pub-sub.png kvspool.html
DITAA=java -jar ditaa0_9.jar -o

reader-writer.png: reader-writer.txt
	$(DITAA) $<

pub-sub.png: pub-sub.txt
	$(DITAA) $<

kvspool.html: kvspool.txt
	asciidoc -a toc2 $<

.PHONY: clean

clean:
	rm -f kvspool.html
