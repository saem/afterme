package app

import (
	"fmt"
	"github.com/saem/afterme/data"
	"github.com/saem/afterme/data1"
	"log"
	"time"
)

// TODO: These should be defaults for config read off the App struct
const (
	MaxMessageSize         = 50 * 1024 * 1024 // Bytes
	MaxWriteBufferSize     = 100              //MaxMessageSize * MaxWriteBufferSize ~ total memory consumption
	MaxResponseBufferSize  = MaxWriteBufferSize
	WriteCoalescingTimeout = 1000 * 2 * time.Millisecond
	MaxBytesPerFile        = 1024 * 1024 * 1024 //Default 1GB, soft limit
)

// App is the protocol agnostic core of the application
type App struct {
	Sequence   data.Sequence
	Version    data.Version
	DataDir    string
	DataWriter chan WriteRequest
	Logger     *log.Logger
	dataFile   data.DataFile
}

// CreateAppServer creates a properly initialized App instance
func CreateAppServer(dataDir string, logger *log.Logger, sequence data.Sequence) (appServer *App) {
	appServer = new(App)
	appServer.Sequence = sequence
	appServer.Version = 1
	appServer.DataDir = dataDir
	appServer.DataWriter = make(chan WriteRequest, MaxWriteBufferSize)
	appServer.Logger = logger
	appServer.createFile()

	return appServer
}

// WriteRequest is sent this to the App.DataWriter channel to request that Body is written,
// and a notification (WriteResponse) sent via WriteRequest.Notify
type WriteRequest struct {
	Body   []byte
	Notify chan WriteResponse
}

// WriteResponse struct sent back to notify a requester of a write as to what happened
type WriteResponse struct {
	Sequence data.Sequence
	Notify   chan WriteResponse
	Err      error
}

// RequestWrite lines up a piece of data to be written to the data log
func (app *App) RequestWrite(Body []byte) (notifier chan WriteResponse) {
	notifier = make(chan WriteResponse)

	// We add a new line to body to ensure that the next header cleanly starts on the new line
	request := WriteRequest{Body: append(Body, '\n'), Notify: notifier}

	app.DataWriter <- request
	return notifier
}

// createFile creates the actual file, on the file system, for writing.
// This should probably be put into the data1 package
func (app *App) createFile() {
	if app.dataFile != nil {
		err := app.dataFile.Close()
		if err != nil {
			fmt.Print("butts")
		}
	}

	app.dataFile = data1.NewDataFile(app.Sequence, app.DataDir)

	err := app.dataFile.CreateForWrite()
	if err != nil {
		app.Logger.Fatalf("Could not open file, %s/%s, for writing. because: %s",
			app.dataFile.Name(),
			app.DataDir,
			err.Error())
	}
}

type WriteResponseBuffer struct {
	buf         []WriteResponse
	outstanding uint32
}

// createResponseBuffer creates a properly initialized buffer, based on config parameters
func createResponseBuffer() (buf *WriteResponseBuffer) {
	buf = new(WriteResponseBuffer)
	buf.buf = make([]WriteResponse, MaxWriteBufferSize, MaxWriteBufferSize)
	buf.outstanding = 0

	return buf
}

// buffer is meant to buffer a write response, and return an error when the buffer is full
func (buf *WriteResponseBuffer) buffer(res WriteResponse) (err error) {
	buf.buf[buf.outstanding] = res
	buf.outstanding++

	if buf.outstanding == MaxResponseBufferSize {
		return fmt.Errorf("Response buffer full, will fail on call.")
	}

	return nil
}

// ProcessMessages is a single writer that completes all WriteRequests, flushing them, and notifying of commits
func (app *App) ProcessMessages() {
	writeCoalesceTimeout := time.Tick(WriteCoalescingTimeout)
	writeResponses := createResponseBuffer()
	for {
		if app.dataFile.BytesWritten() >= MaxBytesPerFile {
			app.flushResponses(writeResponses)
			app.createFile()
		}

		select {
		case writeRequest := <-app.DataWriter:
			// This part should probably be controlled by the file format

			message := data1.Message{Sequence: app.Sequence,
				TimeStamp:   time.Now().Unix(),
				MessageSize: uint32(len(writeRequest.Body)),
				Body:        writeRequest.Body}

			var writeResponse WriteResponse

			err := app.dataFile.Write(message)

			if err != nil {
				writeResponse = WriteResponse{Sequence: message.Sequence,
					Notify: writeRequest.Notify,
					Err:    err}

				writeResponse.Notify <- writeResponse
			} else {
				// The last thing we do is append, effectively marking the end of the transaction
				writeResponse = WriteResponse{Sequence: message.Sequence,
					Notify: writeRequest.Notify,
					Err:    nil}

				err = writeResponses.buffer(writeResponse)
				if err != nil {
					app.flushResponses(writeResponses)
				}

				app.Sequence++
			}

		case <-writeCoalesceTimeout:
			app.flushResponses(writeResponses)
		}
	}

	app.dataFile.Close()
}

// flushResponses syncs and informs all pending requests that their data is "safe"
func (app *App) flushResponses(writeResponses *WriteResponseBuffer) {
	app.dataFile.Sync()
	for i := uint32(0); i < writeResponses.outstanding; i++ {
		safeNotify(writeResponses.buf[i])
	}

	writeResponses.outstanding = 0
}

// safeNotify is used to inform the write requester, while avoiding issues with a closed channel
func safeNotify(wr WriteResponse) {
	defer recover()
	wr.Notify <- wr
}
