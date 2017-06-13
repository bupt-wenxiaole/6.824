package main

import (
	"fmt"
	"mapreduce"
	"os"
	// "regexp"
	// "strconv"
	// "strings"
)

func mapF(filename string, contents string) []mapreduce.KeyValue {
	return NULL
}

func reduceF(key string, values []string) string {
	return ""
}

func main() {
	if len(os.Args) < 3 {
		fmt.Printf("Wrong number of args!\n")
	} else {
		// mapreduce.StartWorkerServer(os.Args[1], os.Args[2], mapF, reduceF, os.Args[3])
		mapreduce.StartWorkerServer("master", "/var/tmp/824-1000/mr4259-worker", mapF, reduceF, 0)
	}
}
