:: This is a sample batch file for executing the dbloadgen CLI on Windows.
:: It will be made more robust later.

set clijar=".\cli\dbloadgencli-1.0-SNAPSHOT-jar-with-dependencies.jar"
set javaexe="%JAvA_HOME%\bin\java.exe"

%javaexe% ^
   -Dfile.encoding=UTF-8  -Doracle.jdbc.javaNetNio=false ^
   -classpath %clijar% ^
   qlikpe.dbloadgen.DbLoadgenCLI %*
 

