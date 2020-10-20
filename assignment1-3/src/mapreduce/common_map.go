package mapreduce

import (
	"fmt"
	"log"
	"os"
	"hash/fnv"
	"io/ioutil"
	"encoding/json"
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

	fileContentBytes, err := ioutil.ReadFile(inFile) // just pass the file name
	if err != nil {
		log.Fatal(err)
	}
	fileContent := string(fileContentBytes)
	var fileName string
	var keyValuesSlice []KeyValue
	keyValuesSlice = mapF(inFile, fileContent)
	intermediateFiles :=  make(map[string] *os.File)
	for reduceTaskCounter := 0; reduceTaskCounter < nReduce; reduceTaskCounter++ {
		fileName = reduceName(jobName, mapTaskNumber, reduceTaskCounter)
		fWriter, err := os.Create(fileName)
		if err != nil {
			return
		}
		defer fWriter.Close()
		intermediateFiles[fileName] = fWriter
	}
	for _, kv := range keyValuesSlice {
		reduceTaskCounter := int(ihash(kv.Key)) % nReduce
		fileName = reduceName(jobName, mapTaskNumber, reduceTaskCounter)
		enc := json.NewEncoder(intermediateFiles[fileName])
		err := enc.Encode(&kv)
		if err != nil {
			fmt.Println(err)
		}
	}
}

func ihash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
