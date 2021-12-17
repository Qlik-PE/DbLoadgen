#
scripts=dbloadgen.sh dbloadgen.bat dbloadgenserver.sh dbloadgenserver.bat
testscripts=baseballtest.sh h2test.sh oracletest.sh postgrestest.sh saptest.sh sqltest.sh h2test.bat
clijar=cli/target/dbloadgencli-1.0-SNAPSHOT-jar-with-dependencies.jar
serverjar=server/target/dbloadgenserver-1.0-SNAPSHOT.jar
zipdir=release

rebuild: clean common package

.PHONY: release common docker

clean:
	mvn clean

common:
	mvn -pl common install # puts the common jar in the local .m2 repo

package:
	mvn package

release: rebuild
	rm -rf $(zipdir)/*
	mkdir -p $(zipdir)/dbloadgen/cli/
	mkdir -p $(zipdir)/dbloadgen/server/
	cp $(scripts) $(zipdir)/dbloadgen
	cp $(testscripts) $(zipdir)/dbloadgen
	cp -R ./datasets $(zipdir)/dbloadgen
	cp  $(clijar) $(zipdir)/dbloadgen/cli/
	cp  $(serverjar) $(zipdir)/dbloadgen/server/
	cd $(zipdir) && zip -r dbloadgen.zip ./dbloadgen

docker:
	cd docker && make docker-release
