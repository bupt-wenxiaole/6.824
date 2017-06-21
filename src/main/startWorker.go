package main

import (
	"fmt"
	"log"
	"mapreduce"
	"os"
	"strconv"
	"strings"
	"unicode"
)

// the map function which will be execurated by the workers
func mapF(filename string, contents string) (res []mapreduce.KeyValue) {
	values := strings.FieldsFunc(contents, func(c rune) bool {
		return !unicode.IsLetter(c)
	})
	for _, v := range values {
		res = append(res, mapreduce.KeyValue{v, "1"})
	}
	return res
}

// the reduce function which will be execurated by the workers
func reduceF(key string, values []string) string {
	var sum int

	for _, v := range values {
		count, err := strconv.Atoi(v)
		if err != nil {
			log.Fatal("reduceF: strconv string to int err: ", err)
		}
		sum += count
	}
	return strconv.Itoa(sum)
}

func main() {
	if len(os.Args) < 3 {
		fmt.Printf("Wrong number of args!\n")
	} else {
		n, _ := strconv.Atoi(os.Args[3])
		mapreduce.RunWorker(os.Args[1], os.Args[2], mapF, reduceF, n)
	}
}
