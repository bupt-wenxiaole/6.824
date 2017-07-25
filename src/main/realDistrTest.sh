#!/bin/bash

# set the IP of slaves and the ssh user name
slaveIp="127.0.0.1"
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
go run startMaster.go wcd 127.0.0.1:7769 3 pg-*.txt > logMaster.txt&

# copy the code to the slave node
# ssh ${slaveName}@${slaveIp} "rm -r ${slavePath}"
# ssh ${slaveName}@${slaveIp} "mkdir -p ${slavePath}/src"
# scp -r ${masterPath}/src/main ${slaveName}@${slaveIp}:${slavePath}/src
# scp -r ${masterPath}/src/mapreduce ${slaveName}@${slaveIp}:${slavePath}/src

# start shell script to start the worker daemon
ssh ${slaveName}@${slaveIp} "source /root/.bash_profile"
ssh ${slaveName}@${slaveIp} "chmod +x /root/6.824/src/main/startWorker.sh"
ssh ${slaveName}@${slaveIp} "/root/6.824/src/main/startWorker.sh"

# check the output file
#outputFile=${slavePath}/src/main/mrtmp.wcd
#while true
#do
#	if [ -f ${outputFile} ]; then
#		break
#	fi
#done

# test the output
#cd ${masterPath}/src/main
#sort -n -k 2 mrtmp.wcd | tail -10 | diff - -b mr-testout.txt > diff.out
#if [ -s diff.out ]
#then
#echo "Failed test. Output should be as in mr-testout.txt. Your output differs as follows (from diff.out):" > /dev/stderr
#  cat diff.out
#else
#  echo "Passed test" > /dev/stderr
#fi

# remove the intermediate files
# rm mrtmp*