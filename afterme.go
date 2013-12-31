package main

import (
	"fmt"
	"github.com/saem/afterme/app"
	"github.com/saem/afterme/data"
	"github.com/saem/afterme/data1"
	"github.com/saem/afterme/server"
	"io/ioutil"
	"log"
	"os"
)

func main() {
	dataDir := "./data-dir"
	logger := log.New(os.Stdout, "", log.LstdFlags)
	sequence := data.Sequence(1)

	latestFile, err := findLatestFile(dataDir)

	if err != nil {
		latestFile = data1.NewDataFile(sequence, dataDir)
	}

	scanner, err := latestFile.OpenForRead()
	if err != nil {
		logger.Fatalf("Could not open file, %s/%s, for reading. because: %s",
			latestFile.Name(),
			dataDir,
			err.Error())
	}

	for i := 0; scanner.Scan(); i++ {
		// Every odd row is a header
		if i%2 == 0 {
			sequence = data1.MessageFromHeader(scanner.Text()).Sequence
		}
	}

	defer latestFile.Close()

	var appServer = app.CreateAppServer(dataDir, logger, sequence)

	go appServer.ProcessMessages()

	err = server.Start("localhost:4000", appServer)

	if err != nil {
		appServer.Logger.Fatalf("Could not start http server: %s", err.Error())
	}
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
		case data1.LogFileValidateName(fileInfo.Name()):
			var fileStartingSequence data.Sequence
			version, fileStartingSequence, err = data1.LogFileNameParser(fileInfo.Name())

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
		return data1.NewDataFile(sequence, dataDir), nil
	}

	return nil, data.DataFileError{Name: latestFile.Name(), Code: data.NO_FILES_FOUND}
}
