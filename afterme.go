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
	DataWriter chan WriteRequest
	logger     *log.Logger
}

type WriteRequest struct {
	Body   []byte
	Notify chan WriteResponse
}

type WriteResponse struct {
	Sequence data.Sequence
	Notify   chan WriteResponse
	Err      error
}

var app = new(App)

func main() {
	app.Sequence = 0
	app.Version = 1
	app.DataDir = "./data-dir"
	app.DataWriter = make(chan WriteRequest, MaxWriteBufferSize)
	app.logger = log.New(os.Stdout, "", log.LstdFlags)

	latestFile, err := findLatestFile(app.DataDir)

	if err != nil {
		latestFile = data1.NewDataFile(app.Sequence, app.DataDir)
	}

	err = latestFile.OpenForRead()
	if err != nil {
		app.logger.Fatalf("Could not open file, %s/%s, for reading. because: %s",
			latestFile.Name(),
			app.DataDir,
			err.Error())
	}
	defer latestFile.Close()

	fmt.Printf("We opened, %s, to find the last sequence\n", latestFile.Name())

	// TODO: determine the starting sequence

	go app.ProcessMessages()

	// TODO: Move this into an http server interface
	http.HandleFunc("/message", messageHandler)
	http.HandleFunc("/status", statusHandler)
	http.HandleFunc("/health", healthHandler)
	http.HandleFunc("/", http.NotFound)
	http.ListenAndServe("localhost:4000", nil)
}

func (app *App) ProcessMessages() {
	writeCoalesceTimeout := time.Tick(WriteCoalescingTimeout)
	var writeResponses []WriteResponse
	var writeRequest WriteRequest
	for {
		select {
		// TODO: Handle case where there are many WriteRequests in flight and need to be flushed
		case writeRequest = <-app.DataWriter:
			app.Sequence++

			// This part should probably be controlled by the file format

			message := data1.Message{Sequence: app.Sequence,
				TimeStamp:   time.Now().Unix(),
				MessageSize: uint32(len(writeRequest.Body)),
				Body:        writeRequest.Body}

			header, body, err := message.Marshal()
			if err != nil {
				app.logger.Panicf("Marshalling error, this should never have happened")
			}

			fmt.Printf("Received a message, header: %sbody: %s\n", header, body)

			// The last thing we do is append, effectively marking the end of the transaction
			writeResponses = append(writeResponses, WriteResponse{Sequence: message.Sequence,
				Notify: writeRequest.Notify,
				Err:    nil})

		case <-writeCoalesceTimeout:
			fmt.Println("Write coalescing timeout")
			fmt.Println("Pretend we called flush")
			for _, writeResponse := range writeResponses {
				// TODO: handle case where the Notify channel is closed
				writeResponse.Notify <- writeResponse
			}

			writeResponses = make([]WriteResponse, 0)
		}
	}
}

func (app *App) RequestWrite(Body []byte) (notifier chan WriteResponse) {
	notifier = make(chan WriteResponse)
	request := WriteRequest{Body: Body, Notify: notifier}
	app.DataWriter <- request
	return notifier
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

	notifier := app.RequestWrite(body)
	writeResponse := <-notifier
	if writeResponse.Err != nil {
		fmt.Fprintf(w, "Something went wrong when writing")
	} else {
		fmt.Fprintf(w, "Successfully written, sequence: %s", writeResponse.Sequence)
	}
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
