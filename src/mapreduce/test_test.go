package mapreduce

import (
	"bufio"
	"fmt"
	"log"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
	"bytes"
	"github.com/Alluxio/alluxio-go/option"
)

const (
	nNumber = 100000
	nMap    = 100
	nReduce = 50
	Read = "read"
	Write = "write"
)

// Create input file with N numbers
// Check if we have N numbers in output file

// Split in words
func MapFunc(file string, value string) (res []KeyValue) {
	//debug("Map %v\n", value)
	words := strings.Fields(value)
	for _, w := range words {
		kv := KeyValue{w, ""}
		res = append(res, kv)
	}
	return
}

// Just return key
func ReduceFunc(key string, values []string) string {
	for _, e := range values {
		debug("Reduce %s %v\n", key, e)
	}
	return ""
}


// Checks input file agaist output file: each input number should show up
// in the output file in string sorted order
func check(t *testing.T, files []string) {
	//TODO:修改访问方式
	//output, err := os.Open("mrtmp.test")
	fs := SetUpClient("10.2.152.24")
	outputId, err := fs.OpenFile("/mrtmp.test", &option.OpenFile{})
	defer fs.Close(outputId)
	if err != nil {
		log.Fatal(err)
	}
	if err != nil {
		log.Fatal("check: ", err)
	}
	output, err := fs.Read(outputId)
	if err != nil {
		log.Fatal()
	}

	//defer output.Close()

	var lines []string
	for _, f := range files {
		inputId, err := fs.OpenFile(f, &option.OpenFile{})
		if err !=nil {
			log.Fatal(err)
		}
		input, err := fs.Read(inputId)
		if err != nil {
			log.Fatal("check: ", err)
		}
		fs.Close(inputId)
		inputScanner := bufio.NewScanner(input)
		for inputScanner.Scan() {
			lines = append(lines, inputScanner.Text())
		}
	}

	sort.Strings(lines)

	outputScanner := bufio.NewScanner(output)
	i := 0
	for outputScanner.Scan() {
		var v1 int
		var v2 int
		text := outputScanner.Text()
		n, err := fmt.Sscanf(lines[i], "%d", &v1)
		if n == 1 && err == nil {
			n, err = fmt.Sscanf(text, "%d", &v2)
		}
		if err != nil || v1 != v2 {
			t.Fatalf("line %d: %d != %d err %v\n", i, v1, v2, err)
		}
		i++
	}
	if i != nNumber {
		t.Fatalf("Expected %d lines in output\n", nNumber)
	}
}

// Workers report back how many RPCs they have processed in the Shutdown reply.
// Check that they processed at least 1 DoTask RPC.
func checkWorker(t *testing.T, l []int) {
	for _, tasks := range l {
		if tasks == 0 {
			t.Fatalf("A worker didn't do any work\n")
		}
	}
}

// Make input file
func makeInputs(num int) []string {
	var names []string
	var i = 0
	for f := 0; f < num; f++ {
		names = append(names, fmt.Sprintf("/test/824-mrinput-%d.txt", f))
		//TODO:修改创建方式
		//file, err := os.Create(names[f])
		fs := SetUpClient("10.2.152.24")
		createId, err := fs.CreateFile(names[f], &option.CreateFile{})
		if err != nil {
			log.Fatal("mkInput: ", err)
		}
		//TODO
		//w := bufio.NewWriter(file)
		byteArray := []byte{}
		for i < (f+1)*(nNumber/num) {
			byteArray = append(byteArray, byte(i))
			i++
		}
		_, err = fs.Write(createId, bytes.NewBuffer(byteArray))
		if err != nil {
			log.Fatal(err)
		}
		fs.Close(createId)
		//TODO
		//w.Flush()
		//file.Close()

	}
	return names
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp. can't use current directory since
// AFS doesn't support UNIX-domain sockets.
func port(suffix string) string {
	//s := "/var/tmp/824-"
	//s += strconv.Itoa(os.Getuid()) + "/"
	//os.Mkdir(s, 0777)
	//s += "mr"
	//s += strconv.Itoa(os.Getpid()) + "-"
	//s += suffix
	s := "10.2.152.24"
	return s + suffix
}

func setup() *Master {
	files := makeInputs(nMap)
	master := port(":1234")
	mr := distributed("test", files, nReduce, master)
	return mr
}

func cleanup(mr *Master) {
	mr.cleanupFiles()
	for _, f := range mr.files {
		removeFile(f)
	}
}

func TestSequentialSingle(t *testing.T) {
	mr := Sequential("test", makeInputs(1), 1, MapFunc, ReduceFunc)
	mr.wait()
	check(t, mr.files)
	checkWorker(t, mr.stats)
	cleanup(mr)
}

func TestSequentialMany(t *testing.T) {
	mr := Sequential("test", makeInputs(5), 3, MapFunc, ReduceFunc)
	mr.wait()
	check(t, mr.files)
	checkWorker(t, mr.stats)
	cleanup(mr)
}

func TestBasic(t *testing.T) {
	mr := setup()
	/*for i := 0; i < 2; i++ {
		//go RunWorker(mr.address, port("worker"+strconv.Itoa(i)),
		go RunWorker(mr.address, port(":"+strconv.Itoa(1500+i)),
			MapFunc, ReduceFunc, -1)
	}*/
	//let three workers run
	go RunWorker("10.2.152.21", port(":"+strconv.Itoa(7778)), MapFunc, ReduceFunc, -1)
	go RunWorker("10.2.152.22", port(":"+strconv.Itoa(7778)), MapFunc, ReduceFunc, -1)
	go RunWorker("10.2.152.24", port(":"+strconv.Itoa(7778)), MapFunc, ReduceFunc, -1)
	mr.wait()
	check(t, mr.files)
	checkWorker(t, mr.stats)
	cleanup(mr)
}

func TestOneFailure(t *testing.T) {
	mr := setup()
	// Start 2 workers that fail after 10 tasks
	go RunWorker(mr.address, port("worker"+strconv.Itoa(0)),
		MapFunc, ReduceFunc, 10)
	go RunWorker(mr.address, port("worker"+strconv.Itoa(1)),
		MapFunc, ReduceFunc, -1)
	mr.wait()
	check(t, mr.files)
	checkWorker(t, mr.stats)
	cleanup(mr)
}

func TestManyFailures(t *testing.T) {
	mr := setup()
	i := 0
	done := false
	for !done {
		select {
		case done = <-mr.doneChannel:
			check(t, mr.files)
			cleanup(mr)
			break
		default:
			// Start 2 workers each sec. The workers fail after 10 tasks
			w := port("worker" + strconv.Itoa(i))
			go RunWorker(mr.address, w, MapFunc, ReduceFunc, 10)
			i++
			w = port("worker" + strconv.Itoa(i))
			go RunWorker(mr.address, w, MapFunc, ReduceFunc, 10)
			i++
			time.Sleep(1 * time.Second)
		}
	}
}
