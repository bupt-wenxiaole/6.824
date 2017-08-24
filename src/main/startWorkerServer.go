package main

import (
	"mapreduce"
	"strings"
	"unicode"
	"strconv"
	"os"
	"fmt"
)

func mapF(document string, value string) (res[]mapreduce.KeyValue) {
	res = make([]mapreduce.KeyValue, 0)
	ss := strings.FieldsFunc(value, func(c rune) bool {
		return !unicode.IsLetter(c)
})
	for _, s := range ss {
		res = append(res, mapreduce.KeyValue{s, "1"})
	}
	return
}

func reduceF(key string, values []string) string {
	return strconv.Itoa(len(values))
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