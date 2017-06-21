package main

import (
	"fmt"
	"github.com/colinmarc/hdfs"
)

func main() {
	client, err := hdfs.New("hadoopmaster:9000")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer client.Close()

	fileRd, err := client.Open("/user/hadoop/data/out/test")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer fileRd.Close()

	buffer := make([]byte, 100)
	_, err = fileRd.Read(buffer)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(buffer)
}
