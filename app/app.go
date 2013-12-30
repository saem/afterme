package app

import (
	"fmt"
	"github.com/saem/afterme/data"
	"github.com/saem/afterme/data1"
	"log"
	"time"
)

// These should be defaults for config read off the App struct
const (
	MaxMessageSize         = 50 * 1024 * 1024 // Bytes
	MaxWriteBufferSize     = 100              //MaxMessageSize * MaxWriteBufferSize ~ total memory consumption
	WriteCoalescingTimeout = 1000 * 2 * time.Millisecond
)

// The protocol agnostic core of the application
type App struct {
	Sequence   data.Sequence
	Version    data.Version
	DataDir    string
	DataWriter chan WriteRequest
	Logger     *log.Logger
}

// Creates a properly initialized App instance
func CreateAppServer(dataDir string, logger *log.Logger, sequence data.Sequence) (appServer *App) {
	appServer = new(App)
	appServer.Sequence = sequence
	appServer.Version = 1
	appServer.DataDir = dataDir
	appServer.DataWriter = make(chan WriteRequest, MaxWriteBufferSize)
	appServer.Logger = logger

	return appServer
}

// Send this to the App.DataWriter channel to request that Body is written,
// and a notification (WriteResponse) sent via WriteRequest.Notify
type WriteRequest struct {
	Body   []byte
	Notify chan WriteResponse
}

// The struct sent back to notify a requester of a write as to what happened
type WriteResponse struct {
	Sequence data.Sequence
	Notify   chan WriteResponse
	Err      error
}

// Call this to enqueue a new write to the log
func (app *App) RequestWrite(Body []byte) (notifier chan WriteResponse) {
	notifier = make(chan WriteResponse)
	request := WriteRequest{Body: Body, Notify: notifier}
	app.DataWriter <- request
	return notifier
}

// The single writer that completes all WriteRequests, flushing them and notifying of commits
func (app *App) ProcessMessages() {
	writeCoalesceTimeout := time.Tick(WriteCoalescingTimeout)
	var writeResponses []WriteResponse
	var writeRequest WriteRequest
	for {
		select {
		// TODO: Handle case where there are many WriteRequests in flight and need to be flushed
		case writeRequest = <-app.DataWriter:
			// This part should probably be controlled by the file format

			message := data1.Message{Sequence: app.Sequence,
				TimeStamp:   time.Now().Unix(),
				MessageSize: uint32(len(writeRequest.Body)),
				Body:        writeRequest.Body}

			header, body, err := message.Marshal()
			if err != nil {
				app.Logger.Panicf("Marshalling error, this should never have happened")
			}

			app.Sequence++

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
