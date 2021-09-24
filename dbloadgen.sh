#! /bin/bash
#
# A shell scripts that wraps the java-based dbloadgen CLI
#
# All arguments from the command line are passed along to the dbloadgen CLI main().
#

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
#  1. Specified in the DBLOADGENCLI_JAR environment variable
#  2. Somwhere in the current directory (recursive)
#  3. Hope it is in the classpath somewhere. NOTE: This is NOT optimal unless it
#     is the only jar in the classpath. Otherwise there could be dependency issues.
if [ -z "${DBLOADGENCLI_JAR}" ]
then
   DBLOADGENCLI_JAR="$(find . -name "dbloadgencli*jar-with-dependencies.jar" -print0 | xargs -0 ls -t | head -1)"
   if [ -z "${DBLOADGENCLI_JAR}" ]
   then
      if [ -z "${CLASSPATH}" ]
      then
         echo "ERROR: dbloadgencli jar file not found in current directory and CLASSPATH is not set."
         exit 1
      else
         echo "WARN: dbloadgencli jar file not found in current directory. Using CLASSPATH."
         DBLOADGENCLI_JAR="${CLASSPATH}"
      fi
   fi
fi

# -Doracle.jdbc.javaNetNio=false prevents "Socket read interrupted" exceptions
#    in Oracle. See this article: 
#       https://support.oracle.com/knowledge/Middleware/2612009_1.html
#       if you have a My Oracle Support account, or
#       https://knowledge.broadcom.com/external/article/205920/jdbc-errors-in-wildfly-javanetsockettime.html
#
$JAVA_PATH -Dfile.encoding=UTF-8  -Doracle.jdbc.javaNetNio=false \
   -classpath "${DBLOADGENCLI_JAR}" \
   qlikpe.dbloadgen.DbLoadgenCLI "$@"


