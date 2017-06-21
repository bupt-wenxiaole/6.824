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
		n, _ := strconv.Atoi(os.Args[3])
		mapreduce.RunMaster(os.Args[1], getFiles(os.Args[4]), n, os.Args[2])
	}
}
