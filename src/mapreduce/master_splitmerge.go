package mapreduce

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/Alluxio/alluxio-go/option"
	"log"
	"sort"
)

// merge combines the results of the many reduce jobs into a single output file
// XXX use merge sort
func (mr *Master) merge() {
	fs := SetUpClient("10.2.152.24")
	debug("Merge phase")
	kvs := make(map[string]string)
	for i := 0; i < mr.nReduce; i++ {
		p := mergeName(mr.jobName, i)
		fmt.Printf("Merge: read %s\n", p)
		//file, err := os.Open(p)
		readId, err := fs.OpenFile("/test/"+p, &option.OpenFile{})
		if err != nil {
			log.Fatal("Merge: ", err)
		}
		buff, err := fs.Read(readId)
		dec := json.NewDecoder(buff)
		for {
			var kv KeyValue
			err = dec.Decode(&kv)
			if err != nil {
				break
			}
			kvs[kv.Key] = kv.Value
		}
		//file.Close()
		fs.Close(readId)
	}
	var keys []string
	for k := range kvs {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	//file, err := os.Create("mrtmp." + mr.jobName)
	createId, err := fs.CreateFile("/test/mrtmp."+mr.jobName, &option.CreateFile{})
	if err != nil {
		log.Fatal("Merge: create ", err)
	}
	//w := bufio.NewWriter(file)
	var buff bytes.Buffer
	for _, k := range keys {
		fmt.Fprintf(&buff, "%s: %s\n", k, kvs[k])
	}
	_, err = fs.Write(createId, &buff)
	fs.Close(createId)
}

// removeFile is a simple wrapper around os.Remove that logs errors.
func removeFile(n string) {
	//err := os.Remove(n)
	fs := SetUpClient("10.2.152.24")
	err := fs.Delete("/test/"+n, &option.Delete{})
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
