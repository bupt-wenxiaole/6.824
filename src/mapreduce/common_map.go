package mapreduce

import (
	"hash/fnv"
	"fmt"
	"github.com/Alluxio/alluxio-go/option"
	"log"
	"io/ioutil"
	"github.com/json-iterator/go"
	"bytes"
	"time"
)

// doMap does the job of a map worker: it reads one of the input files
// (inFile), calls the user-defined map function (mapF) for that file's
// contents, and partitions the output into nReduce intermediate files.
func doMap(
	jobName string, // the name of the MapReduce job
	mapTaskNumber int, // which map task this is
	inFile string,
	nReduce int, // the number of reduce task that will be run ("R" in the paper)
	mapF func(file string, contents string) []KeyValue,
) {
	// TODO:
	// You will need to write this function.
	// You can find the filename for this map task's input to reduce task number
	// r using reduceName(jobName, mapTaskNumber, r). The ihash function (given
	// below doMap) should be used to decide which file a given key belongs into.
	//
	// The intermediate output of a map task is stored in the file
	// system as multiple files whose name indicates which map task produced
	// them, as well as which reduce task they are for. Coming up with a
	// scheme for how to store the key/value pairs on disk can be tricky,
	// especially when taking into account that both keys and values could
	// contain newlines, quotes, and any other character you can think of.
	//
	// One format often used for serializing data to a byte stream that the
	// other end can correctly reconstruct is JSON. You are not required to
	// use JSON, but as the output of the reduce tasks *must* be JSON,
	// familiarizing yourself with it here may prove useful. You can write
	// out a data structure as a JSON string to a file using the commented
	// code below. The corresponding decoding functions can be found in
	// common_reduce.go.
	//
	//   enc := json.NewEncoder(file)
	//   for _, kv := ... {
	//     err := enc.Encode(&kv)
	//
	// Remember to close the file after you have written all the values!
	/*
		type FileInfo interface {
			Name() string       // base name of the file
			Size() int64        // length in bytes for regular files; system-dependent for others
			Mode() FileMode     // file mode bits
			ModTime() time.Time // modification time
			IsDir() bool        // abbreviation for Mode().IsDir()
			Sys() interface{}   // underlying data source (can return nil)
		}
	*/
	fs := SetUpClient("10.2.152.24")
	readId, err := fs.OpenFile("/test/"+inFile, &option.OpenFile{})
	if err != nil {
		log.Fatal("doMap: read file from alluxio: ", err)
	}
	fileHandle, err := fs.Read(readId)
	if err != nil {
		log.Fatal(err)
	}
	fs.Close(readId)
	contents, err := ioutil.ReadAll(fileHandle)
	if err != nil {
		log.Fatal(err)
	}
	kv := mapF(inFile,string(contents))
	filesenc := make([]*jsoniter.Encoder,nReduce)
	ioBuff := make([]bytes.Buffer, nReduce)
	file := make([]int, nReduce)
	fmt.Println("create map file at ",time.Now().Format("2006-01-02 15:04:05"))
	for i := range filesenc {
		file[i], err = fs.CreateFile("/test/"+reduceName(jobName, mapTaskNumber, i), &option.CreateFile{})
		if err != nil {
			fmt.Printf("%s Create Failed\n",reduceName(jobName, mapTaskNumber, nReduce))
		} else {
			//fmt.Printf("%s Created\n",reduceName(jobName, mapTaskNumber, nReduce))
			filesenc[i] = jsoniter.NewEncoder(&ioBuff[i])
		}
	}
	fmt.Println("json-iterator encode at ",time.Now().Format("2006-01-02 15:04:05"))
	for _,v := range kv {
		err := filesenc[ihash(v.Key) % uint32(nReduce)].Encode(&v)
		if err != nil {
			fmt.Printf("%s Encode Failed %v\n",v,err)
		}
	}
	fmt.Println("write result to map file at ",time.Now().Format("2006-01-02 15:04:05"))
	for j:= 0;j < nReduce;j++ {
		_, err = fs.Write(file[j], &ioBuff[j])
		if err != nil {
			log.Fatal(err)
		}
		fs.Close(file[j])
	}

}

func ihash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

