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

func getFiles() []string {
	ret := []string{"test1.txt"}
	return ret
}

func main() {
	if len(os.Args) < 3 {
		fmt.Println("Wrong number of args!")
	} else {
		mr := mapreduce.Distributed("test", getFiles(), 1, "127.0.0.1:7777")
		mr.Wait()
		// mr.CleanupFiles()
		fmt.Println("MapReduce task finishes.")
	}
}
