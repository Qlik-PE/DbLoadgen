#! /bin/bash
#
# Executes a simple test against an in memory H2 instance.
#

DATASET_DIR="./datasets"
CONNECTION=oratest
WORKLOAD="baseball"


cmd="./dbloadgen.sh --dataset-dir $DATASET_DIR --connection-name $CONNECTION --workload-config $WORKLOAD"

$cmd test-connection
$cmd cleanup 
$cmd init 
$cmd preload
$cmd run

#$cmd end-to-end


