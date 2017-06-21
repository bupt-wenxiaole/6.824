package mapreduce

import (
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/colinmarc/hdfs"
	"log"
	"os"
	"sort"
	"strconv"
)

// merge combines the results of the many reduce jobs into a single output file
// XXX use merge sort
func (mr *Master) merge() {
	client, err := hdfs.New("hadoopmaster:9000")
	if err != nil {
		log.Fatal("doReduce: connect to HDFS: ", err)
	}
	defer client.Close()
	debug("Merge phase")
	kvs := make(map[string]string)
	for i := 0; i < mr.nReduce; i++ {
		p := mergeName(mr.jobName, i)
		// fmt.Printf("Merge: read %s\n", p)
		tmpName := os.Getenv("GOPATH") + "/src/main/mrtmp.merge_" + strconv.Itoa(i)
		err = client.CopyToLocal(p, tmpName)
		if err != nil {
			log.Fatal("merge: copy file from HDFS: ", err)
		}
		defer os.Remove(tmpName)

		file, err := os.Open(tmpName)
		if err != nil {
			log.Fatal("Merge: ", err)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			err = dec.Decode(&kv)
			if err != nil {
				break
			}
			kvs[kv.Key] = kv.Value
		}
		file.Close()
	}
	var keys []string
	for k := range kvs {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	file, err := client.Create("/user/hadoop/data/output/" + "mrtmp." + mr.jobName)
	if err != nil {
		log.Fatal("Merge: create ", err)
	}
	w := bufio.NewWriter(file)
	for _, k := range keys {
		fmt.Fprintf(w, "%s: %s\n", k, kvs[k])
	}
	w.Flush()
	file.Close()
}

// removeFile is a simple wrapper around os.Remove that logs errors.
func removeFile(n string) {
	err := os.Remove(n)
	if err != nil {
		log.Fatal("CleanupFiles ", err)
	}
}

// CleanupFiles removes all intermediate files produced by running mapreduce.
func (mr *Master) cleanupFiles() {
	for i := range mr.files {
		for j := 0; j < mr.nReduce; j++ {
			removeFile(reduceName(mr.jobName, i, j))
		}
	}
	for i := 0; i < mr.nReduce; i++ {
		removeFile(mergeName(mr.jobName, i))
	}
	removeFile("mrtmp." + mr.jobName)
}
