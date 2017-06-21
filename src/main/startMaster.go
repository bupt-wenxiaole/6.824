package main

import (
	"fmt"
	"github.com/colinmarc/hdfs"
	"mapreduce"
	"os"
	"strconv"
	"strings"
)

func getFiles(prefix string) (ret []string) {
	client, err := hdfs.New("hadoopmaster:9000")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer client.Close()

	files, err := client.ReadDir("/user/hadoop/data/input")
	for _, oneFile := range files {
		if strings.HasPrefix(oneFile.Name(), prefix) {
			ret = append(ret, "/user/hadoop/data/input/"+oneFile.Name())
		}
	}

	files, err = client.ReadDir("/user/hadoop/data/out")
	for _, oneFile := range files {
		if strings.HasPrefix(oneFile.Name(), "mrtmp") {
			err = client.Remove("/user/hadoop/data/out/" + oneFile.Name())
			if err != nil {
				fmt.Println(err)
				return
			}
		}
	}

	files, err = client.ReadDir("/user/hadoop/data/output")
	for _, oneFile := range files {
		if strings.HasPrefix(oneFile.Name(), "mrtmp") {
			err = client.Remove("/user/hadoop/data/output/" + oneFile.Name())
			if err != nil {
				fmt.Println(err)
				return
			}
		}
	}

	return
}

func main() {
	if len(os.Args) != 5 {
		fmt.Println("Wrong number of args!")
	} else {
		// os.Args[3:] represent the slice of input files transfered from shell script
		// the list of input files can also be get from the function getFiles
		n, _ := strconv.Atoi(os.Args[3])
		mr := mapreduce.Distributed(os.Args[1], getFiles(os.Args[4]), n, os.Args[2])
		mr.Wait()
		fmt.Println("MapReduce task finishes.")
	}
}
