#! /bin/bash
#
# Executes a simple test against an in memory H2 instance.
#

DATASET_DIR="./datasets"
CONNECTION=h2mem
WORKLOAD="test"


cmd="./dbloadgen.sh --dataset-dir $DATASET_DIR --connection-name $CONNECTION --workload-config $WORKLOAD"

#$cmd test-connection
#$cmd init
#$cmd preload
#$cmd run
#$cmd cleanup

$cmd end-to-end


