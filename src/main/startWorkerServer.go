package main

import (
	"fmt"
	"mapreduce"
	"os"
	// "regexp"
	// "strconv"
	// "strings"
)

func mapF(filename string, contents string) (ret []mapreduce.KeyValue) {
	return
}

func reduceF(key string, values []string) string {
	return ""
}

func main() {
	if len(os.Args) < 3 {
		fmt.Printf("Wrong number of args!\n")
	} else {
		// mapreduce.RunWorker(os.Args[1], os.Args[2], mapF, reduceF, os.Args[3])
		mapreduce.StartWorkerServer("127.0.0.1:7777", "127.0.0.1:7778", mapF, reduceF, -1)
	}
}
