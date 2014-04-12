all: reader-writer.png pub-sub.png index.html
DITAA=java -jar ditaa0_9.jar -o

reader-writer.png: reader-writer.txt
	$(DITAA) $<

pub-sub.png: pub-sub.txt
	$(DITAA) $<

index.html: index.txt
	asciidoc -a toc2 $<

TMP=/tmp/kvspool-gh-pages
stage:
	mkdir -p ${TMP}
	rm -if ${TMP}/*
	cp LICENSE.txt ${TMP}
	cp *.html ${TMP}
	cp *.png ${TMP}

.PHONY: clean

clean:
	rm -f index.html
