#!/bin/bash

# set the IP of slaves and the ssh user name
slaveIp1="10.2.152.24"
slaveIp2="10.2.152.22"
slaveIp3="10.2.152.21"
slaveName=$(whoami)
# slavePath="/home/${slaveName}/go"

# set the golang working directory containing the code
source ~/.bash_profile
slavePath=$GOPATH
masterPath=$GOPATH

# set the environment variable for golang
# export GOPATH=${masterPath}

# change the relative directory so the input files can be open correctly
cd ${masterPath}/src/main

# start the master daemon and return immediately
# the first para is the name of the job
# the second para is the address of the master
# the third para is the number of reduce tasks
# the last para is the list of input files
/usr/local/go/bin/go run startMaster.go test 10.2.152.24:7777 2 pg* > logMaster.txt&

# copy the code to the slave node
# ssh ${slaveName}@${slaveIp} "rm -r ${slavePath}"
# ssh ${slaveName}@${slaveIp} "mkdir -p ${slavePath}/src"
# scp -r ${masterPath}/src/main ${slaveName}@${slaveIp}:${slavePath}/src
# scp -r ${masterPath}/src/mapreduce ${slaveName}@${slaveIp}:${slavePath}/src

# start shell script to start the worker daemon
ssh ${slaveName}@${slaveIp1} "chmod +x ${slavePath}/src/main/startWorker.sh"
ssh ${slaveName}@${slaveIp1} "${slavePath}/src/main/startWorker.sh"

ssh ${slaveName}@${slaveIp2} "chmod +x ${slavePath}/src/main/startWorker.sh"
ssh ${slaveName}@${slaveIp2} "${slavePath}/src/main/startWorker.sh"

ssh ${slaveName}@${slaveIp3} "chmod +x ${slavePath}/src/main/startWorker.sh"
ssh ${slaveName}@${slaveIp3} "${slavePath}/src/main/startWorker.sh"


