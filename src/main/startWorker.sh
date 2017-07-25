#!/bin/bash

# set the environment variable
source ~/.bash_profile
#slavePath=$GOPATH
slave = $1
# export GOPATH=$slavePath

# change the relative directory
#cd ${slavePath}/src/main
cd /root/6.824/src/main

# the list below can be changed to start several worker server simultaneously
#for index in 1 2 3 4
#do
# start a daemon and return immediately
#go run startWorkerServer.go 127.0.0.1:7769 127.0.0.1:777${index} -1 &
#done
#/usr/local/go/bin/go run startWorkerServer.go 10.2.152.24:7769 10.2.152.24:7771 -1 &
# start another daemon so that the sh can only return after the workers are shutdown
# the first para is the master's address
# the second para is the slave's address
# the third para is used to control that after how many steps the slave will shutdown automatically
#go run startWorkerServer.go 127.0.0.1:7769 127.0.0.1:7770 -1
/usr/local/go/bin/go run startWorkerServer.go 10.2.152.24:7777 ${slave}:7778 -1