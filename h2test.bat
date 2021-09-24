REM An example script showing how to configure things on windows. 
REM This script assumes that OpenJDK or later is installed.

set DATASET_DIR=.\datasets
set CONNECTION=h2mem
set WORKLOAD=test


set cmd=.\dbloadgen.bat --dataset-dir %DATASET_DIR% --connection-name %CONNECTION% --workload-config %WORKLOAD%
echo CMD: %cmd% 


:: %cmd% test-connection
:: %cmd% init
:: %cmd% preload
:: %cmd% run
:: %cmd% cleanup

call %cmd% end-to-end



