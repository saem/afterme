package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"regexp"
	"strconv"
)

type Sequence uint64

func main() {
	logger := log.New(os.Stdout, "", log.LstdFlags)
	dataDir := "./data-dir"
	//var sequence Sequence = 0
	
	latestFile, err := findLatestFile(dataDir)
	
	if err != nil {
		logger.Fatalf("Could not find latest file, error: %s", err.Error())
	}
	
	fmt.Printf("We should open this file: %s\n", latestFile.Name())
}

// Log file management

func findLatestFile(dataDir string) (f os.FileInfo, err error) {
	fileInfos, err := ioutil.ReadDir(dataDir)
	if(err != nil) {
		return nil, err
	}
	var sequence Sequence = 0
	
	// Find the latest sequence before we start
	var latestFile os.FileInfo
	// Look for data files <version>-<sequence>.log, maybe others in the future, version must be first
	for _, fileInfo := range fileInfos {
		if fileInfo.IsDir() {
			continue
		}
		
		switch {
			case logFileValidateName1(fileInfo.Name()):
				_, fileStartingSequence, err := logFileNameParser1(fileInfo.Name())
				
				// This shouldn't be possible because we've validated the file name -- famous last words
				if err != nil {
					return nil, err
				}
				
				if fileStartingSequence > sequence {
					sequence = fileStartingSequence
					latestFile = fileInfo
				}
			default:
				continue
		}
	}
	
	return latestFile, nil
}

// Versioned log file name parsing and validation

var validFileName1 = regexp.MustCompile(`^1-(\d+).log$`)

func logFileValidateName1(fileName string) (valid bool) {
	return validFileName1.MatchString(fileName)
}

func logFileNameParser1(fileName string) (version string, sequence Sequence, err error) {
	sequenceString := validFileName1.FindStringSubmatch(fileName)[1]
	currentSequence, err := strconv.ParseUint(sequenceString, 10, 64)
	
	if err != nil {
		return "", 0, fmt.Errorf("Could not parse filename, %s", fileName)
	}
	
	return "1", Sequence(currentSequence), nil
}