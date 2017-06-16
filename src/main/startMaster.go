package main

import (
	"fmt"
	"mapreduce"
	"os"
	"strconv"
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
		n, _ := strconv.Atoi(os.Args[3])
		mr := mapreduce.Distributed(os.Args[1], os.Args[4:], n, os.Args[2])
		mr.Wait()
		fmt.Println("MapReduce task finishes.")
	}
}
