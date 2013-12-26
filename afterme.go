package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"regexp"
	"strconv"
)

// Core data types, used in read/writing, and operational observation
type Sequence uint64
type Version uint64
type DataFile interface {
	CreateForWrite() error
	OpenForRead() error
	Close() error
	Name() string
}
type DataFileErrorCode int
const (
	ALREADY_OPEN    DataFileErrorCode = 1 << iota
	ALREADY_CREATED
	FILE_CLOSED
	NO_FILES_FOUND
)
type DataFileError struct {
	Name string
	Code DataFileErrorCode
}
func (e DataFileError) Error() string {
	var fmtStr = ""
	switch {
		case e.Code == ALREADY_OPEN:
			fmtStr = "File (%s) already open"
		case e.Code == ALREADY_CREATED:
			fmtStr = "File (%s) already created"
		case e.Code == FILE_CLOSED:
			fmtStr = "File (%s) closed"
	}
	
	return fmt.Sprintf(fmtStr, e.Name)
}

func main() {
	logger := log.New(os.Stdout, "", log.LstdFlags)
	dataDir := "./data-dir"
	//var sequence Sequence = 0
	
	latestFile, err := findLatestFile(dataDir)
	
	if err != nil {
		logger.Fatalf("Could not find latest file, error: %s", err.Error())
	}
	
	err = latestFile.OpenForRead()
	if err != nil {
		logger.Fatalf("Could not open file, %s, for reading", latestFile.Name())
	}
	defer latestFile.Close()
	
	fmt.Printf("We opened, %s,file to find the last sequence\n", latestFile.Name())
}

// Log file management/anti-corruption layer between versioned file handling

func findLatestFile(dataDir string) (df DataFile, err error) {
	fileInfos, err := ioutil.ReadDir(dataDir)
	if(err != nil) {
		return nil, err
	}
	var sequence Sequence = 0
	var version Version = Version(0)
	
	// Find the latest sequence before we start
	var latestFile os.FileInfo
	// Look for data files <version>-<sequence>.log, maybe others in the future, version must be first
	for _, fileInfo := range fileInfos {
		if fileInfo.IsDir() {
			continue
		}
		
		switch {
			case logFileValidateName1(fileInfo.Name()):
				var fileStartingSequence Sequence
				version, fileStartingSequence, err = logFileNameParser1(fileInfo.Name())
				
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
	
	switch {
		case version == Version(1):
			return DataFileForExisting1(version, sequence, dataDir), nil
	}
	
	return nil, DataFileError{Name: latestFile.Name(), Code: NO_FILES_FOUND}
}

// Versioned log file name parsing and validation

// Data Structures to support version 1 file format
type dataFile1 struct {
	version Version
	startingSequence Sequence
	dataDir string
	file *os.File
}

func DataFileForExisting1(version Version, startingSequence Sequence, dataDir string) (df *dataFile1) {
	df = new(dataFile1)
	df.version = version
	df.startingSequence = startingSequence
	df.dataDir = dataDir
	
	return df
}

func (df *dataFile1) CreateForWrite() (err error) {
	if df.file != nil {
		return DataFileError{df.Name(), ALREADY_OPEN}
	}
	df.file, err = os.OpenFile(df.fullName(), os.O_APPEND | os.O_CREATE | os.O_EXCL, 0644)
	
	return err
}

func (df *dataFile1) OpenForRead() (err error) {
	if df.file != nil {
		return DataFileError{df.Name(), ALREADY_OPEN}
	}
	df.file, err = os.OpenFile(df.fullName(), os.O_RDONLY, 0644)
	
	return err
}

func (df *dataFile1) Close() (err error) {
	err = nil
	if df.file != nil {
		err = df.file.Close()
	}
	
	df.file = nil //we only allow reading XOR writing, clearing so we can use it as a mutex
	
	return err
}

func (df dataFile1) Name() string {
	return fmt.Sprintf("%d-%d.log", df.version, df.startingSequence)
}

func (df dataFile1) fullName() string {
	return fmt.Sprintf("%s/%s", df.dataDir, df.Name())
}

var validFileName1 = regexp.MustCompile(`^1-(\d+).log$`)

func logFileValidateName1(fileName string) (valid bool) {
	return validFileName1.MatchString(fileName)
}

func logFileNameParser1(fileName string) (version Version, sequence Sequence, err error) {
	sequenceString := validFileName1.FindStringSubmatch(fileName)[1]
	currentSequence, err := strconv.ParseUint(sequenceString, 10, 64)
	
	if err != nil {
		return Version(0), Sequence(0), fmt.Errorf("Could not parse filename, %s", fileName)
	}
	
	return Version(1), Sequence(currentSequence), nil
}