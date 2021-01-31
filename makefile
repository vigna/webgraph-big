include build.properties

TAR=tar

source:
	rm -fr webgraph-big-$(version)
	ant clean	
	ln -s . webgraph-big-$(version)
	$(TAR) chvf webgraph-big-$(version)-src.tar --owner=0 --group=0 \
		webgraph-big-$(version)/README.md \
		webgraph-big-$(version)/CHANGES \
		webgraph-big-$(version)/COPYING.LESSER \
		webgraph-big-$(version)/LICENSE-2.0.txt \
		webgraph-big-$(version)/build.xml \
		webgraph-big-$(version)/build.properties \
		webgraph-big-$(version)/ivy.xml \
		webgraph-big-$(version)/webgraph-big.bnd \
		webgraph-big-$(version)/pom-model.xml \
		webgraph-big-$(version)/src/overview.html \
		webgraph-big-$(version)/src/it/unimi/dsi/big/webgraph/*.{java,html} \
		webgraph-big-$(version)/src/it/unimi/dsi/big/webgraph/labelling/*.{java,html} \
		webgraph-big-$(version)/src/it/unimi/dsi/big/webgraph/algo/*.{java,html} \
		webgraph-big-$(version)/src/it/unimi/dsi/big/webgraph/test/SpeedTest.java \
		webgraph-big-$(version)/src/it/unimi/dsi/big/webgraph/examples/*.{java,html} \
		$$(find webgraph-big-$(version)/test/it/unimi/dsi/big/webgraph -iname *.java -and -not -iname \*Typed\*) \
		$$(find webgraph-big-$(version)/slow/it/unimi/dsi/big/webgraph -iname *.java) \
		webgraph-big-$(version)/slow/it/unimi/dsi/big/webgraph/cnr-2000*
	gzip -f webgraph-big-$(version)-src.tar
	rm webgraph-big-$(version)

binary:
	rm -fr webgraph-big-$(version)
	$(TAR) zxvf webgraph-big-$(version)-src.tar.gz
	(cd webgraph-big-$(version) && unset CLASSPATH && unset LOCAL_IVY_SETTINGS && ant ivy-clean ivy-setupjars && ant junit && ant clean && ant jar javadoc)
	$(TAR) zcvf webgraph-big-$(version)-bin.tar.gz --owner=0 --group=0 \
		webgraph-big-$(version)/README.md \
		webgraph-big-$(version)/CHANGES \
		webgraph-big-$(version)/COPYING.LESSER \
		webgraph-big-$(version)/LICENSE-2.0.txt \
		webgraph-big-$(version)/webgraph-big-$(version).jar \
		webgraph-big-$(version)/docs
	$(TAR) zcvf webgraph-big-$(version)-deps.tar.gz --owner=0 --group=0 --transform='s|.*/||' $$(find webgraph-big-$(version)/jars/runtime -iname \*.jar -exec readlink {} \;) 

stage:
	rm -fr webgraph-big-$(version)
	$(TAR) zxvf webgraph-big-$(version)-src.tar.gz
	cp -fr bnd webgraph-big-$(version)
	(cd webgraph-big-$(version) && unset CLASSPATH && unset LOCAL_IVY_SETTINGS && ant ivy-clean ivy-setupjars && ant stage)
