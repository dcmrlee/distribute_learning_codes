package mapreduce

import (
	"encoding/json"
	"hash/fnv"
	"io/ioutil"
	"log"
	"os"
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

	/*****************************************************************************
	* Added by Dcmrlee.
	*
	* 1. Read contents from task's input file (not consider large file)
	* 2. Run MapF using file contents
	* 3. Generate intermediate file
	*****************************************************************************/

	debug("Entering doMap - [jobName: %s] - [mapTaskNum: %d] - [inFile: %s] - [nReduce: %d]\n", jobName, mapTaskNumber, inFile, nReduce)

	contents, err := ioutil.ReadFile(inFile)
	if err != nil {
		log.Fatal("mapTaskNum: ", mapTaskNumber, "inFile: ", inFile, "error: ", err)
	}
	debug("Finish Reading inFile\n")

	mapResult := mapF(inFile, string(contents))
	mapResultSize := len(mapResult)
	debug("Map Result Size: %d\n", mapResultSize)

	for i := 0; i < nReduce; i++ {
		intermediateFileName := reduceName(jobName, mapTaskNumber, i)
		debug("intermediateFileName: %s\n", intermediateFileName)
		interFile, err := os.Create(intermediateFileName)
		if err != nil {
			log.Fatal("Failed to Create intermediate File: ", intermediateFileName)
		}
		enc := json.NewEncoder(interFile)
		for j := 0; j < mapResultSize; j++ {
			kv := mapResult[j]
			if ihash(kv.Key)%uint32(nReduce) == uint32(i) {
				err := enc.Encode(&kv)
				if err != nil {
					interFile.Close()
					log.Fatal("Encode error: ", err, "kv: ", kv)
				}
			}
		}
		interFile.Close()
	}
}

func ihash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
