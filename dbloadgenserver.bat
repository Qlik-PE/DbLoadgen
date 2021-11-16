:: This is a sample batch file for executing the dbloadgen server on Windows.
:: The plan is to make it more user friendly / robust later.

set serverjar=".\server\dbloadgenserver-1.0-SNAPSHOT.jar"
set javaexe="%JAvA_HOME%\bin\java.exe"

set port="9090"
set guiuser="admin"
set password="admin"
set datasets=".\datasets"

%javaexe% ^
   -Dfile.encoding=UTF-8  -Doracle.jdbc.javaNetNio=false ^
   -XX:TieredStopAtLevel=1 ^
   -noverify ^
   -Dspring.output.ansi.enabled=always ^
   -Dcom.sun.management.jmxremote ^
   -Dspring.jmx.enabled=true ^
   -Dspring.liveBeansView.mbeanDomain ^
   -Dspring.application.admin.enabled=true ^
   -Dfile.encoding=UTF-8 ^
   -jar %serverjar% ^
   --dbloadgen.directory=%datasets% ^
   --spring.security.user.name=%guiuser% ^
   --spring.security.user.password=%password% ^
   --server.port=%port% ^
   --management.server.port=%port%


