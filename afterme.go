package main

import (
	"flag"
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
	notStupidMain(os.Args)
}

// notStupidMain is a fix for go's moronic way of handling argv, whoever though a global variable than a sane
// parameter pass to the main function was a good idea should have their head checked. Seriously, why create
// more global state, rather than less. Now testing around main is more difficult, congrats, for what benefit?
func notStupidMain(argv []string) {
	// Command Line Parameters/Flags
	flags := flag.NewFlagSet(argv[0], flag.ContinueOnError)
	var dataDir string
	flags.StringVar(&dataDir, "datadir",
		app.DefaultDataDir,
		fmt.Sprintf("Sets the data-dir, defaults to: %s", app.DefaultDataDir))
	var port int
	flags.IntVar(&port, "port",
		server.DefaultPort,
		fmt.Sprintf("Sets the port, defaults to: %d", server.DefaultPort))

	flags.Parse(argv[1:])

	logger := log.New(os.Stdout, "", log.LstdFlags)
	sequence := findLatestSequence(dataDir, logger)

	var appServer = app.CreateAppServer(dataDir, logger, sequence)

	go appServer.ProcessMessages()

	err := server.Start(fmt.Sprintf("localhost:%d", port), appServer)

	if err != nil {
		appServer.Logger.Fatalf("Could not start http server: %s", err.Error())
	}
}

func findLatestSequence(dataDir string, logger *log.Logger) (sequence data.Sequence) {
	sequence = data.Sequence(1)
	latestFile, err := findLatestFile(dataDir)

	if err == nil && latestFile != nil {
		scanner, err := latestFile.OpenForRead()
		if err != nil {
			logger.Fatalf("Could not open file, %s/%s, for reading. because: %s",
				dataDir,
				latestFile.Name(),
				err.Error())
		}
		defer latestFile.Close()

		for i := 0; scanner.Scan(); i++ {
			// Every odd row is a header
			if i%2 == 0 {
				sequence = data1.MessageFromHeader(scanner.Text()).Sequence
			}
		}
		sequence++
	}
	
	return
}

// Log file management/anti-corruption layer between versioned file handling

func findLatestFile(dataDir string) (df data.DataFile, err error) {
	fileInfos, err := ioutil.ReadDir(dataDir)
	if err != nil || len(fileInfos) == 0 {
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
