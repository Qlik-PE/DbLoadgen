#! /bin/bash
# 
# An  example of how to run the dbloadgen image from the command line.
#

PORT="9090"
DATASETS="./datasets"
GUIUSER="admin"
PASSWORD="admin"

CMD="docker run -it --rm  \
       -p $PORT:$PORT \
       -e GUIUSER=$GUIUSER \
       -e PASSWORD=$PASSWORD \
       -e PORT=$PORT \
       -e DATASETS=$DATASETS \
       --name dbloadgen attunitypm/dbloadgen:latest"

echo "Running command: $CMD"

$CMD

