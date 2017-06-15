#!/bin/bash

# set the IP of slaves and the ssh user name
slaveIp="127.0.0.1"
slaveName=$(whoami)
# slavePath="/home/${slaveName}/go"

# set the golang working directory containing the code
slavePath="/home/$(whoami)/go/src/6.824_fork"
masterPath="/home/$(whoami)/go/src/6.824_fork"

# set the environment variable for golang
export GOPATH=${masterPath}

# change the relative directory so the input files can be open correctly
cd ${masterPath}/src/main

# start the master daemon and return immediately
go run startMaster.go master sequential pg-*.txt > logMaster.txt&

# copy the code to the slave node
# ssh ${slaveName}@${slaveIp} "rm -r ${slavePath}"
# ssh ${slaveName}@${slaveIp} "mkdir -p ${slavePath}/src"
# scp -r ${masterPath}/src/main ${slaveName}@${slaveIp}:${slavePath}/src
# scp -r ${masterPath}/src/mapreduce ${slaveName}@${slaveIp}:${slavePath}/src

# start shell script to start the worker daemon
ssh ${slaveName}@${slaveIp} "chmod +x ${slavePath}/src/main/startWorker.sh"
ssh ${slaveName}@${slaveIp} "${slavePath}/src/main/startWorker.sh"

# check the output file
while [true]
do
	if [-f mrtmp.wcd]
		then
		break
	fi
done

# test the output
cd ${masterPath}/src/main
sort -n -k2 mrtmp.wcd | tail -10 | diff - -b mr-testout.txt > diff.out
if [ -s diff.out ]
then
echo "Failed test. Output should be as in mr-testout.txt. Your output differs as follows (from diff.out):" > /dev/stderr
  cat diff.out
else
  echo "Passed test" > /dev/stderr
fi

# remove the intermediate files
rm mrtmp*