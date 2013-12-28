package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"github.com/saem/afterme/data"
	"github.com/saem/afterme/data1"
	"net/http"
)

type App struct {
	Sequence data.Sequence
	Version data.Version
	DataDir string
}

func main() {
	app := App{Sequence: 1, Version: 1, DataDir: "./data-dir"}
	logger := log.New(os.Stdout, "", log.LstdFlags)
	
	latestFile, err := findLatestFile(app.DataDir)

	if err != nil {
		latestFile = data1.NewDataFile1(app.Sequence, app.DataDir)
	}

	err = latestFile.OpenForRead()
	if err != nil {
		logger.Fatalf("Could not open file, %s/%s, for reading. because: %s",
			      latestFile.Name(),
			      app.DataDir,
		err.Error())
	}
	defer latestFile.Close()

	fmt.Printf("We opened, %s, to find the last sequence\n", latestFile.Name())
	
	// TODO: determine the starting sequence
	
	http.HandleFunc("/message", messageHandler)
	http.HandleFunc("/status", statusHandler)
	http.HandleFunc("/health", healthHandler)
	http.HandleFunc("/", http.NotFound)
	http.ListenAndServe("localhost:4000", nil)
	
}

func messageHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "Let's pretend we wrote some data.")
}

func statusHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "Return the current status.")
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "Return the result of health checks.")
}

// Log file management/anti-corruption layer between versioned file handling

func findLatestFile(dataDir string) (df data.DataFile, err error) {
	fileInfos, err := ioutil.ReadDir(dataDir)
	if err != nil {
		return nil, err
	}
	var sequence data.Sequence = 0
	var version data.Version = data.Version(0)

	// Find the latest sequence before we start
	var latestFile os.FileInfo
	// Look for data files <version>-<sequence>.log, maybe others in the future, version must be first
	for _, fileInfo := range fileInfos {
		if fileInfo.IsDir() {
			continue
		}

		switch {
		case data1.LogFileValidateName1(fileInfo.Name()):
			var fileStartingSequence data.Sequence
			version, fileStartingSequence, err = data1.LogFileNameParser1(fileInfo.Name())

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
	case version == data.Version(1):
		return data1.NewDataFile1(sequence, dataDir), nil
	}

	return nil, data.DataFileError{Name: latestFile.Name(), Code: data.NO_FILES_FOUND}
}
