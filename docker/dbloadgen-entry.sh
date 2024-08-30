#! /bin/bash
#
# A shell script that wraps the java-based dbloadgen Server and CLI
#

# Default values for command line arguments.
PORT=${PORT:-"9090"}
DATASETS=${DATASETS:-"./datasets"}
GUIUSER=${GUIUSER:-"admin"}
PASSWORD=${PASSWORD:-"admin"}
CMD=""


server() {
   #
   # Locate the jar file we need.
   #  1. Specified in the DBLOADGENSERVER_JAR environment variable
   #  2. Somwhere in the current directory (recursive)
   #  3. Hope it is in the classpath somewhere. NOTE: This is NOT optimal unless it
   #     is the only jar in the classpath. Otherwise there could be dependency issues.
   if [ -z "${DBLOADGENSERVER_JAR}" ]
   then
      DBLOADGENSERVER_JAR="$(find . -name "dbloadgenserver*SNAPSHOT.jar")"
      if [ -z "${DBLOADGENSERVER_JAR}" ]
      then
         echo "ERROR: dbloadgenserver jar file not found in current directory."
         exit 1
      fi
   fi
   
   echo "Using settings: PORT=$PORT USER=$GUIUSER PASSWORD=$PASSWORD DATASETS=$DATASETS"
   echo "Using jar file at: $DBLOADGENSERVER_JAR"
   
   
   # -Doracle.jdbc.javaNetNio=false prevents "Socket read interrupted" exceptions
   #    in Oracle. See this article: 
   #       https://support.oracle.com/knowledge/Middleware/2612009_1.html
   #       if you have a My Oracle Support account, or
   #       https://knowledge.broadcom.com/external/article/205920/jdbc-errors-in-wildfly-javanetsockettime.html
   #
   CMD="exec java -Dfile.encoding=UTF-8  -Doracle.jdbc.javaNetNio=false \
      -XX:TieredStopAtLevel=1 \
      -noverify \
      -Dspring.output.ansi.enabled=always \
      -Dcom.sun.management.jmxremote \
      -Dspring.jmx.enabled=true \
      -Dspring.liveBeansView.mbeanDomain \
      -Dspring.application.admin.enabled=true \
      -Dfile.encoding=UTF-8 \
      -jar ${DBLOADGENSERVER_JAR} \
      --dbloadgen.directory=${DATASETS} \
      --spring.security.user.name=${GUIUSER} \
      --spring.security.user.password=${PASSWORD} \
      --server.port=${PORT} \
      --management.server.port=${PORT}" 
}

cli() {
   #
   # Locate the jar file we need.
   #  1. Specified in the DBLOADGENCLI_JAR environment variable
   #  2. Somwhere in the current directory (recursive)
   #  3. Hope it is in the classpath somewhere. NOTE: This is NOT optimal unless it
   #     is the only jar in the classpath. Otherwise there could be dependency issues.
   if [ -z "${DBLOADGENCLI_JAR}" ]
   then
      DBLOADGENCLI_JAR="$(find . -name "dbloadgencli-1.0-SNAPSHOT-jar-with-dependencies.jar")"
      if [ -z "${DBLOADGENCLI_JAR}" ]
      then
         echo "ERROR: dbloadgencli jar file not found in current directory."
         exit 1
      fi
   fi
   
   echo "Using settings: PORT=$PORT USER=$GUIUSER PASSWORD=$PASSWORD DATASETS=$DATASETS"
   echo "Using jar file at: $DBLOADGENCLI_JAR"
   
   
   # -Doracle.jdbc.javaNetNio=false prevents "Socket read interrupted" exceptions
   #    in Oracle. See this article: 
   #       https://support.oracle.com/knowledge/Middleware/2612009_1.html
   #       if you have a My Oracle Support account, or
   #       https://knowledge.broadcom.com/external/article/205920/jdbc-errors-in-wildfly-javanetsockettime.html
   #
   CMD="exec java -Dfile.encoding=UTF-8  -Doracle.jdbc.javaNetNio=false \
      -classpath ${DBLOADGENCLI_JAR} \
      qlikpe.dbloadgen.DbLoadgenCLI --dataset-dir ${DATASETS} $*"
}

if [ $# -eq 0 ] 
then
   server
else
   case $1 in
      "server")
         server
         ;;
      "cli")
         shift
         cli "$@"
         ;;
      *) 
         echo "ERROR: invalid command speciffied: $1"
         exit 1
         ;;
   esac
fi

echo "running command: $CMD"
${CMD}
