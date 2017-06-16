#!/bin/bash

# set the environment variable
source ~/.bash_profile
slavePath=$GOPATH
# export GOPATH=$slavePath

# change the relative directory
cd ${slavePath}/src/main

# the list below can be changed to start several worker server simultaneously
for index in 1 2 3 4
do
# start a daemon and return immediately
go run startWorkerServer.go 127.0.0.1:7769 127.0.0.1:777${index} -1 &
done

# start another daemon so that the sh can only return after the workers are shutdown
# the first para is the master's address
# the second para is the slave's address
# the third para is used to control that after how many steps the slave will shutdown automatically
go run startWorkerServer.go 127.0.0.1:7769 127.0.0.1:7770 -1