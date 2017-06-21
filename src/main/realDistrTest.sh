#!/bin/bash

# set the golang working directory containing the code
source ~/.bash_profile
masterPath=$GOPATH

# change the relative directory so the input files can be open correctly
cd ${masterPath}/src/main

# Get the slaves' names and call them up
for line in $(cat slaves.conf)
do
	ssh hadoop@${line} "source ~/.bash_profile; chmod +x '$GOPATH'/src/main/startWorker.sh; '$GOPATH'/src/main/startWorker.sh ${line}" &
done

# start the master daemon and return immediately
# the first para is the name of the job
# the second para is the address of the master
# the third para is the number of reduce tasks
# the last para is the list of input files
go run startMaster.go wcd hadoopmaster:7777 3 pg