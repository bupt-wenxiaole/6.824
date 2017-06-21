#!/bin/bash

# set the environment variable
source ~/.bash_profile
slavePath=$GOPATH
slaveGo=$GOROOT

slaveName=$1

# change the relative directory
cd ${slavePath}/src/main

# start a daemon
${slaveGo}/bin/go run startWorker.go hadoopmaster:7777 ${slaveName}:7778 -1
