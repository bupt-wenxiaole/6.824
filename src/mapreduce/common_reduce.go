package mapreduce

import (
	"fmt"
	"sort"
	"github.com/Alluxio/alluxio-go/option"
	"log"
	"bytes"
	"io/ioutil"
	"time"
	"github.com/json-iterator/go"
)

// doReduce does the job of a reduce worker: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	outFile string,
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	// TODO:
	// You will need to write this function.
	// You can find the intermediate file for this reduce task from map task number
	// m using reduceName(jobName, m, reduceTaskNumber).
	// Remember that you've encoded the values in the intermediate files, so you
	// will need to decode them. If you chose to use JSON, you can read out
	// multiple decoded values by creating a decoder, and then repeatedly calling
	// .Decode() on it until Decode() returns an error.
	//
	// You should write the reduced output in as JSON encoded KeyValue
	// objects to a file named mergeName(jobName, reduceTaskNumber). We require
	// you to use JSON here because that is what the merger than combines the
	// output from all the reduce tasks expects. There is nothing "special" about
	// JSON -- it is just the marshalling format we chose to use. It will look
	// something like this:
	//
	// enc := json.NewEncoder(mergeFile)
	// for key in ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	fs := SetUpClient("10.2.152.24")
	keyValues := make(map[string][]string)
	ioBuff := make([]bytes.Buffer, nMap)
	//fmt.Println("decode and do reduce at ",time.Now().Format("2006-01-02 15:04:05"))
	for i := 0; i < nMap; i++ {
		fileId, err := fs.OpenFile("/test/"+reduceName(jobName, i, reduceTaskNumber), &option.OpenFile{})
		//file,err := os.Open(reduceName(jobName, i, reduceTaskNumber))
		if err != nil {
			fmt.Printf("reduce file:%s can't open\n",reduceName(jobName, i, reduceTaskNumber))
		} else {
			stream, err := fs.Read(fileId)
			if err != nil {
				log.Fatal("doReduce:read file from alluxio: ", err)
			}
			out, err := ioutil.ReadAll(stream)
			ioBuff[i].Write(out)
			enc := jsoniter.NewDecoder(&ioBuff[i])
			for {
				var kv KeyValue
				err := enc.Decode(&kv)
				if err != nil {
					break  // 此时文件解码完毕
				}
				_, ok := keyValues[kv.Key]
				if !ok { // 说明当前并没有这个key
					keyValues[kv.Key] = make([]string, 0)
				}
				keyValues[kv.Key] = append(keyValues[kv.Key], kv.Value)
			}
			_, err = fs.Write(fileId, &ioBuff[i])
			fs.Close(fileId)
		}
	}
	var keys []string
	for k, _ := range keyValues {
		keys = append(keys, k)
	}
	sort.Strings(keys)  // 递增排序

	fileId,err := fs.CreateFile("/test/"+mergeName(jobName, reduceTaskNumber), &option.CreateFile{})
	if err != nil {
		fmt.Printf("reduce merge file:%s can't open\n",mergeName(jobName, reduceTaskNumber))
		return
	}
	var ioBuff1 bytes.Buffer
	enc := jsoniter.NewEncoder(&ioBuff1)
	//fmt.Println("encode reduce result at ",time.Now().Format("2006-01-02 15:04:05"))
	for _,k := range keys {
		enc.Encode(KeyValue{k, reduceF(k,keyValues[k])})
	}
	_, err = fs.Write(fileId, &ioBuff1)
	fs.Close(fileId)
	//fmt.Println("finish reduce at ",time.Now().Format("2006-01-02 15:04:05"))
}

