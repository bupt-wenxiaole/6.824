#!/bin/bash

# set the environment variable
slavePath="/home/$(whoami)/go/src/6.824_fork"
export GOPATH=$slavePath

# change the relative directory
cd $GOPATH/src/main

# the list below can be changed to start several worker server simultaneously
for index in 1 2 3 4
do
# start a daemon and return immediately
go run startWorkerServer.go 127.0.0.1:7769 127.0.0.1:777${index} 2 &
done

# start another daemon so that the sh can only return after the workers are shutdown
go run startWorkerServer.go 127.0.0.1:7769 127.0.0.1:7770 -1