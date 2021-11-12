#! /bin/bash
#
# A shell script that wraps the java-based dbloadgen Server
#

# Default values for command line arguments.
PORT=${PORT:-"9090"}
DATASETS=${DATASETS:-"./datasets"}
GUIUSER=${GUIUSER:-"admin"}
PASSWORD=${PASSWORD:-"admin"}

if [ "$#" -ne 0 ]
then
   for arg in "$@"
   do
      case "$arg" in
        --port=*)
           PORT="${arg#*=}"
          ;;
        --user=*)
           GUIUSER="${arg#*=}"
          ;;
        --password=*)
           PASSWORD="${arg#*=}"
          ;;
        --datasets=*)
           DATASETS="${arg#*=}"
           # make sure the directory path is valid
           if [ ! -d "${DATASETS}" ]
           then
              echo "*** ERROR: --datasets=${DATASETS} is not a valid path"
              exit 1
           fi
           ;;
        --help|-h)
           ;&
        *)
           echo "Usage: dbloadgenserver.sh [--help] [--port=<port>] [--user=<username>]"
           echo "                          [--password=<password>] [--datasets=<path>]"
           echo "         --port=<port>: override the default port number (9090)"
           echo "         --user=<username>: override the default login username for the GUI (admin)"
           echo "         --password=<password>: override the default password for the GUI (admin)"
           echo "         --datasets=<path>: path to a directory containing dataset,"
           echo "                                connection, and workload metadata (/datasets)"
           echo "         --help: print this message"
           exit 1
           ;;
      esac
   done
fi

echo "Using settings: PORT=$PORT USER=$GUIUSER PASSWORD=$PASSWORD DATASETS=$DATASETS"


#
# Make sure we have Java installed
#
if [ -z "${JAVA_HOME}" ]
then
   JAVA_PATH=$(which java 2>/dev/null)
   if [ -z "${JAVA_PATH}" ]
   then
      echo "ERROR: java not found in your path and may not be installed"
      exit 1
   fi
else 
   JAVA_PATH="$JAVA_HOME/bin/java"
   if [ ! -f "$JAVA_PATH" ]
   then
      echo "ERROR: JAVA_HOME ($JAVA_PATH) points to an invalid location or is not readable"
      exit 1
   fi
fi

#
# Locate the jar file we need.
#  1. Specified in the DBLOADGENSERVER_JAR environment variable
#  2. Somwhere in the current directory (recursive)
#  3. Hope it is in the classpath somewhere. NOTE: This is NOT optimal unless it
#     is the only jar in the classpath. Otherwise there could be dependency issues.
if [ -z "${DBLOADGENSERVER_JAR}" ]
then
   DBLOADGENSERVER_JAR="$(find . -name "dbloadgenserver*SNAPSHOT.jar" -print0 | xargs -0 ls -t | head -1)"
   if [ -z "${DBLOADGENSERVER_JAR}" ]
   then
      echo "ERROR: dbloadgenserver jar file not found in current directory and CLASSPATH is not set."
      exit 1
   fi
fi

# -Doracle.jdbc.javaNetNio=false prevents "Socket read interrupted" exceptions
#    in Oracle. See this article: 
#       https://support.oracle.com/knowledge/Middleware/2612009_1.html
#       if you have a My Oracle Support account, or
#       https://knowledge.broadcom.com/external/article/205920/jdbc-errors-in-wildfly-javanetsockettime.html
#
$JAVA_PATH -Dfile.encoding=UTF-8  -Doracle.jdbc.javaNetNio=false \
   -XX:TieredStopAtLevel=1 \
   -noverify \
   -Dspring.output.ansi.enabled=always \
   -Dcom.sun.management.jmxremote \
   -Dspring.jmx.enabled=true \
   -Dspring.liveBeansView.mbeanDomain \
   -Dspring.application.admin.enabled=true \
   -Dfile.encoding=UTF-8 \
   -jar "${DBLOADGENSERVER_JAR}" \
   --dbloadgen.directory="${DATASETS}" \
   --spring.security.user.name="${GUIUSER}" \
   --spring.security.user.password="${PASSWORD}" \
   --server.port="${PORT}" \
   --management.server.port="${PORT}" 

