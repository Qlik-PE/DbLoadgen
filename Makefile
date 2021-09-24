#
scripts=dbloadgen.sh dbloadgen.bat
testscripts=baseballtest.sh h2test.sh oracletest.sh postgrestest.sh saptest.sh sqltest.sh h2test.bat
clijar=cli/target/dbloadgencli-1.0-SNAPSHOT-jar-with-dependencies.jar
zipdir=release



rebuild: clean common package

clean:
	mvn clean

common:
	mvn -pl common install # puts the common jar in the local .m2 repo

package:
	mvn package

.PHONY: release

release:
	rm -rf $(zipdir)/*
	mkdir -p $(zipdir)/dbloadgen/cli/
	cp $(scripts) $(zipdir)/dbloadgen
	cp $(testscripts) $(zipdir)/dbloadgen
	cp -R ./datasets $(zipdir)/dbloadgen
	cp  $(clijar) $(zipdir)/dbloadgen/cli/
	cd $(zipdir) && zip -r dbloadgen.zip ./dbloadgen
