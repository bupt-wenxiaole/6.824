package mapreduce

import (
	"os"
	"encoding/json"
	"bufio"
)

// doReduce manages one reduce task: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string,       // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	outFile string,       // write the output here
	nMap int,             // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {

	mergeFileName := mergeName(jobName, reduceTaskNumber) //每个reduce任务的结果存储在各自的这个文件里。
	//最后这些文件可以进行merge也可以不进行merge
	mergeFileHandle, err := os.Create(mergeFileName)
	CheckError(err)
	defer mergeFileHandle.Close()
	mergeFileEnc := json.NewEncoder(mergeFileHandle)
	reduceFuncInput := make(map[string][]string)
	for i := 0; i < nMap; i ++ {
		tmpfileListToReduce := reduceName(jobName, i, reduceTaskNumber)
		tmpfileListToReduceHandle, err := os.Open(tmpfileListToReduce)
		CheckError(err)
		defer tmpfileListToReduceHandle.Close()
		tmpfileScanner := bufio.NewScanner(tmpfileListToReduceHandle)
		//下面的代码为什么要从mrtmp-test-i-j文件json解码到结构体中再写入文件：文件中使用json格式下面的英文注释说的很明白，json便于数据的序列化存储
		//在进行reduce操作的时候需要再将json中数据解码到结构体中进行必要的运算处理，处理完的结果再用Json格式写入到文件中
		for tmpfileScanner.Scan() {
			tmpBytes := []byte(tmpfileScanner.Text())
			var kv KeyValue
			err := json.Unmarshal(tmpBytes, &kv)
			CheckError(err)
			reduceFuncInput[kv.Key] = append(reduceFuncInput[kv.Key], kv.Value)
		}

	}
	// enc := json.NewEncoder(file)
	for k, v := range reduceFuncInput {
		//向每个reduce结果文件中写入的也是json格式，key是去重过的key, value是将每个同样的key的value进行合并(reduceF)后的结果
		mergeFileEnc.Encode(KeyValue{k, reduceF(k, v)})
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
