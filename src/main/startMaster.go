package main

import (
	"fmt"
	"mapreduce"
	"os"
	// "regexp"
	// "strconv"
	// "strings"
)

func getFiles() []string {
	ret := []string{
		"test1.txt",
		"test2.txt",
	}
	return ret
}

func main() {
	if len(os.Args) < 3 {
		fmt.Println("Wrong number of args!")
	} else {
		// os.Args[3:] represent the slice of input files transfered from shell script
		// the list of input files can also be get from the function getFiles
		mr := mapreduce.Distributed("wcd", os.Args[3:], 1, "127.0.0.1:7769")
		mr.Wait()
		fmt.Println("MapReduce task finishes.")
	}
}
