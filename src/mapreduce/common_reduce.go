package mapreduce

import (
	"encoding/json"
	"github.com/colinmarc/hdfs"
	"log"
	"os"
	"sort"
	"strconv"
)

// doReduce manages one reduce task: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	client, err := hdfs.New("hadoopmaster:9000")
	if err != nil {
		log.Fatal("doReduce: connect to HDFS: ", err)
	}
	defer client.Close()

	keyValues := make(map[string][]string, 0)
	for i := 0; i < nMap; i++ {
		fileName := reduceName(jobName, i, reduceTaskNumber)
		tmpName := os.Getenv("GOPATH") + "/src/main/mrtmp.doReduce_" + strconv.Itoa(reduceTaskNumber)
		err = client.CopyToLocal(fileName, tmpName)
		if err != nil {
			log.Fatal("doReduce: copy file from HDFS: ", err)
		}
		defer os.Remove(tmpName)

		file, err := os.Open(tmpName)
		if err != nil {
			log.Fatal("doReduce: open intermediate file ", fileName, " error: ", err)
		}
		defer file.Close()

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue

			err := dec.Decode(&kv)
			if err != nil {
				break
			}

			_, ok := keyValues[kv.Key]
			if !ok {
				keyValues[kv.Key] = make([]string, 0)
			}
			keyValues[kv.Key] = append(keyValues[kv.Key], kv.Value)
		}
	}

	var keys []string

	for k, _ := range keyValues {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	mergeFileName := mergeName(jobName, reduceTaskNumber)
	mergeFile, err := client.Create(mergeFileName)
	if err != nil {
		log.Fatal("doReduce: create merge file ", mergeFileName, " error: ", err)
	}
	defer mergeFile.Close()

	enc := json.NewEncoder(mergeFile)
	for _, k := range keys {
		res := reduceF(k, keyValues[k])
		err := enc.Encode(&KeyValue{k, res})
		if err != nil {
			log.Fatal("doReduce: encode error: ", err)
		}
	}

	//
	// You will need to write this function.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTaskNumber) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
}
