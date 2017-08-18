package main

import (
	"fmt"
	"mapreduce"
	"os"
	"regexp"
	"strconv"
	"strings"
)

// the map function which will be execurated by the workers
func mapF(filename string, contents string) []mapreduce.KeyValue {
	components := strings.Fields(contents)
	re := regexp.MustCompile("[A-Za-z]+")
	var res []mapreduce.KeyValue
	for _, c := range components {
		words := re.FindAllString(c, -1) //从components中提取单词
		for _, w := range words {
			kv := mapreduce.KeyValue{w, "1"}
			res = append(res, kv)
		}

	}
	return res
}

// the reduce function which will be execurated by the workers
func reduceF(key string, values []string) string {
	count := 0 //这个key的单词计数
	for _, e := range values {
		i, err := strconv.Atoi(e)
		mapreduce.CheckError(err)
		count += i

	}
	return strconv.Itoa(count)
}

func main() {
	if len(os.Args) < 3 {
		fmt.Printf("Wrong number of args!\n")
	} else {
		// mapreduce.RunWorker(os.Args[1], os.Args[2], mapF, reduceF, os.Args[3])
		// os.Args[1] represent the different ip:port for different worker
		n, _ := strconv.Atoi(os.Args[3])
		mapreduce.RunWorker(os.Args[1], os.Args[2], mapF, reduceF, n)
	}
}
