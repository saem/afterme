package main

import (
	"fmt"
	"github.com/saem/afterme/data"
	"github.com/saem/afterme/data1"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"time"
)

const (
	MaxMessageSize         = 50 * 1024 * 1024 // Bytes
	MaxWriteBufferSize     = 100              //MaxMessageSize * MaxWriteBufferSize ~ total memory consumption
	WriteCoalescingTimeout = 1000 * 2 * time.Millisecond
)

type App struct {
	Sequence   data.Sequence
	Version    data.Version
	DataDir    string
	DataWriter chan data.Message
}

var app = new(App)

func main() {
	app.Sequence = 1
	app.Version = 1
	app.DataDir = "./data-dir"
	app.DataWriter = make(chan data.Message, MaxWriteBufferSize)
	logger := log.New(os.Stdout, "", log.LstdFlags)

	latestFile, err := findLatestFile(app.DataDir)

	if err != nil {
		latestFile = data1.NewDataFile(app.Sequence, app.DataDir)
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
	go dataProcess(app)

	http.HandleFunc("/message", messageHandler)
	http.HandleFunc("/status", statusHandler)
	http.HandleFunc("/health", healthHandler)
	http.HandleFunc("/", http.NotFound)
	http.ListenAndServe("localhost:4000", nil)
}

func dataProcess(app *App) {
	writeCoalesceTimeout := time.Tick(WriteCoalescingTimeout)
	for {
		select {
		case <-app.DataWriter:
			fmt.Println("Received a message")
		case <-writeCoalesceTimeout:
			fmt.Println("Write coalescing timeout")
		}
	}
}

func writeToLog(message data.Message) {
	app.DataWriter <- message
}

func messageHandler(w http.ResponseWriter, r *http.Request) {
	if r.ContentLength < 0 || r.ContentLength > MaxMessageSize {
		fmt.Fprintf(w, "Do some HTTP error code stuff")
	}

	var body []byte
	bytesRead, err := r.Body.Read(body)

	if err != nil && err != io.EOF {
		fmt.Fprintf(w, "Do some HTTP error code stuff, got and error: %s", err.Error())
	}
	if bytesRead != int(r.ContentLength) {
		fmt.Fprintf(w, "Do some HTTP error code stuff, content length incorrect")
	}

	r.Body.Close()

	message := data1.Message{Sequence: 1, TimeStamp: time.Now().Unix(), MessageSize: uint32(bytesRead), Body: body}

	go writeToLog(message)
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
