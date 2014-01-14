package app

import (
	"crypto/sha1"
	"encoding/base64"
	"fmt"
	"github.com/saem/afterme/data"
	"github.com/saem/afterme/data1"
	"io/ioutil"
	"log"
	"time"
)

// TODO: These should be defaults for config read off the App struct

const (
	DefaultDataDir         = "./data-dir"
	MaxMessageSize         = 50 * 1024 * 1024 // Bytes
	MaxUnCommittedWrites   = 1000
	MaxWriteBufferSize     = MaxUnCommittedWrites //MaxMessageSize * MaxWriteBufferSize ~ total memory consumption
	MaxResponseBufferSize  = MaxUnCommittedWrites //Soft limit, could be double at times
	WriteCoalescingTimeout = 2 * time.Millisecond
	MaxBytesPerFile        = 1024 * 1024 * 1024 //Default 1GB, soft limit
)

// App is the protocol agnostic core of the application.
type App struct {
	Sequence   data.Sequence
	Version    data.Version
	DataDir    string
	DataWriter chan WriteRequest
	Logger     *log.Logger
	dataFile   data.DataFile
}

// WriteRequest is sent this to the App.DataWriter channel to request that Body is written,
// and a notification (WriteResponse) sent via WriteRequest.Notify.
type WriteRequest struct {
	Body   []byte
	Notify chan WriteResponse
	Hash   string
}

// WriteResponse struct sent back to notify a requester of a write as to what happened.
type WriteResponse struct {
	Sequence data.Sequence
	Hash     string
	Notify   chan WriteResponse
	Err      error
}

// WriteResponseBuffer is used to keep track of unacknowledged writes.
type WriteResponseBuffer struct {
	buf         []WriteResponse
	outstanding uint32
}

// CreateAppServer creates a properly initialized App instance.
func CreateAppServer(dataDir string, logger *log.Logger) (appServer *App) {
	appServer = new(App)
	appServer.Sequence = findLatestSequence(dataDir, logger)
	appServer.Version = 1
	appServer.DataDir = dataDir
	appServer.DataWriter = make(chan WriteRequest, MaxWriteBufferSize)
	appServer.Logger = logger
	appServer.createFile()

	return appServer
}

// createFile creates the actual file, on the file system, for writing.
// This should probably be put into the data1 package.
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
			app.DataDir,
			app.dataFile.Name(),
			err.Error())
	}
}

// RequestWrite lines up a piece of data to be written to the data log,
// data not ending in a '\n' will have one added.
func (app *App) RequestWrite(body []byte) (notifier chan WriteResponse) {
	notifier = make(chan WriteResponse)

	// We add a new line to body to ensure that the next header cleanly starts on the new line
	if body[len(body)-1] != '\n' {
		body = append(body, '\n')
	}

	h := sha1.New()
	h.Write(body)
	hash := base64.StdEncoding.EncodeToString(h.Sum(nil))

	request := WriteRequest{Body: body, Notify: notifier, Hash: hash}

	app.DataWriter <- request
	return notifier
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
			message := data1.Message{Sequence: app.Sequence,
				TimeStamp:   time.Now().Unix(),
				MessageSize: uint32(len(writeRequest.Body)),
				Hash:        writeRequest.Hash,
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
					Hash:   writeRequest.Hash,
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

// createResponseBuffer creates a properly initialized buffer, based on config parameters
func createResponseBuffer() (buf *WriteResponseBuffer) {
	buf = new(WriteResponseBuffer)
	buf.buf = make([]WriteResponse, MaxWriteBufferSize, MaxWriteBufferSize)
	buf.outstanding = 0

	return buf
}

// buffer is meant to buffer a WriteResponse, returns an error when an insertion fills the buffer
func (buf *WriteResponseBuffer) buffer(res WriteResponse) (err error) {
	buf.buf[buf.outstanding] = res
	buf.outstanding++

	if buf.outstanding == MaxResponseBufferSize {
		return fmt.Errorf("Response buffer full, will fail on call.")
	}

	return nil
}

// flushResponses syncs and informs all pending requests that their data is "safe", completing WriteResponses
func (app *App) flushResponses(writeResponses *WriteResponseBuffer) {
	if writeResponses.outstanding > 0 {
		oldResponses := createResponseBuffer()
		oldResponses.outstanding = writeResponses.outstanding
		copy(oldResponses.buf, writeResponses.buf)
		writeResponses.outstanding = 0

		go func() {
			err := app.dataFile.Sync()
			if err != nil {
				app.Logger.Fatalf("butts, it broke on sync: %s", err.Error())
			}
			for i := uint32(0); i < oldResponses.outstanding; i++ {
				safeNotify(oldResponses.buf[i])
			}
			oldResponses.outstanding = 0
		}()
	}
}

// safeNotify is used to inform the write requester, while avoiding issues with a closed channel
func safeNotify(wr WriteResponse) {
	defer recover()
	wr.Notify <- wr
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
			}
		default:
			continue
		}
	}

	switch {
	case version == data.Version(1):
		return data1.NewDataFile(sequence, dataDir), nil
	}

	return nil, data.DataFileError{Name: "", Code: data.NO_FILES_FOUND}
}
