package data1

import (
	"bufio"
	"fmt"
	"github.com/saem/afterme/data"
	"os"
	"regexp"
	"strconv"
)

// Versioned log file name parsing and validation

const (
	MaxHeaderSize = 20 + 1 + 20 + 1 + 10 + 1 + 28 // bytes
)

// Data Structures to support version 1 file format

type dataFile struct {
	version          data.Version
	startingSequence data.Sequence
	dataDir          string
	file             *os.File
	bytesWritten     uint32
}

type Message struct {
	Sequence    data.Sequence
	TimeStamp   int64
	MessageSize uint32
	Hash        string
	Body        []byte
}

// Marshal creates a header string, and a []byte to be write to disk, it mutates the body by appending a '\n',
// this should be removed on reads
func (message Message) Marshal() (header string, body []byte, err error) {
	header = fmt.Sprintf("%d-%d-%d-%s\n", message.Sequence, message.TimeStamp, message.MessageSize, message.Hash)
	return header, message.Body, nil
}

func (message Message) Unmarshal(header string, body []byte) (err error) {
	m := MessageFromHeader(header)

	message.Sequence = m.Sequence
	message.TimeStamp = m.TimeStamp
	message.MessageSize = m.MessageSize
	message.Hash = m.Hash
	message.Body = body

	return nil
}

func NewDataFile(startingSequence data.Sequence, dataDir string) (df *dataFile) {
	df = new(dataFile)
	df.version = data.Version(1)
	df.startingSequence = startingSequence
	df.dataDir = dataDir

	return df
}

func (df *dataFile) CreateForWrite() (err error) {
	if df.file != nil {
		return data.DataFileError{df.Name(), data.ALREADY_OPEN}
	}
	df.file, err = os.OpenFile(df.fullName(), os.O_APPEND|os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)

	return err
}

func (df *dataFile) OpenForRead() (scanner *bufio.Scanner, err error) {
	if df.file != nil {
		return nil, data.DataFileError{df.Name(), data.ALREADY_OPEN}
	}
	df.file, err = os.OpenFile(df.fullName(), os.O_RDONLY, 0644)

	return df.scanner(), err
}

func (df dataFile) Write(message data.Message) (err error) {
	header, body, err := message.Marshal()
	if err != nil {
		return err
	}

	// TODO: properly handle write errors

	bytesWritten, err := df.file.Write([]byte(header))
	df.bytesWritten += uint32(bytesWritten)
	if err != nil {
		return err
	}

	bytesWritten, err = df.file.Write(body)
	df.bytesWritten += uint32(bytesWritten)
	if err != nil {
		return err
	}

	return nil
}

func (df dataFile) scanner() (scanner *bufio.Scanner) {
	if df.file == nil {
		fmt.Errorf("For some reason the file pointer is nil")
	}

	scanner = bufio.NewScanner(df.file)
	parseHeader := true
	var header string
	split := func(data []byte, atEOF bool) (advance int, token []byte, err error) {
		if parseHeader {
			advance, token, err = bufio.ScanLines(data, atEOF)
			if err == nil && token != nil {
				if !validateMessageHeader(string(token)) {
					err = fmt.Errorf("Malformed header: %s", string(token))
					return
				} else {
					header = string(token)
					parseHeader = false // alternate parsing logic
				}
			}
		} else {
			var messageSize uint64
			messageSizeString := validMessageHeader.FindStringSubmatch(header)[3]
			messageSize, err = strconv.ParseUint(messageSizeString, 10, 32)
			if err != nil {
				err = fmt.Errorf("Could not parse message size from header")
				return
			}

			if uint64(len(data)) < messageSize {
				advance = 0
				token = nil
			} else {
				token = data[:messageSize]
				advance = int(messageSize)
				parseHeader = true // alternate parsing logic
			}
		}

		return
	}
	scanner.Split(split)

	return
}

var validMessageHeader = regexp.MustCompile(`^(\d+)-(\d+)-(\d+)-([a-zA-Z0-9=+/]+)$`)

func MessageFromHeader(header string) (message Message) {
	matches := validMessageHeader.FindStringSubmatch(header)

	// TODO: Throw panic on errors
	sequence, _ := strconv.ParseUint(matches[1], 10, 64)
	timeStamp, _ := strconv.ParseInt(matches[2], 10, 64)
	messageSize, _ := strconv.ParseUint(matches[3], 10, 32)

	// TODO: handle strconv
	message = Message{Sequence: data.Sequence(sequence),
		TimeStamp:   timeStamp,
		MessageSize: uint32(messageSize),
		Hash:        matches[4]}

	return
}

func validateMessageHeader(header string) (valid bool) {
	return validMessageHeader.MatchString(header)
}

func (df dataFile) BytesWritten() (bytes uint32) {
	return df.bytesWritten
}

func (df dataFile) Sync() (err error) {
	return df.file.Sync()
}

func (df dataFile) Close() (err error) {
	if df.file != nil {
		err = df.file.Close()
	}

	df.file = nil //we only allow reading XOR writing

	return err
}

func (df dataFile) Name() string {
	return fmt.Sprintf("%d-%d.log", df.version, df.startingSequence)
}

func (df dataFile) fullName() string {
	return fmt.Sprintf("%s/%s", df.dataDir, df.Name())
}

var validFileName = regexp.MustCompile(`^1-(\d+).log$`)

func LogFileValidateName(fileName string) (valid bool) {
	return validFileName.MatchString(fileName)
}

func LogFileNameParser(fileName string) (version data.Version, sequence data.Sequence, err error) {
	sequenceString := validFileName.FindStringSubmatch(fileName)[1]
	currentSequence, err := strconv.ParseUint(sequenceString, 10, 64)

	if err != nil {
		return data.Version(0), data.Sequence(0), fmt.Errorf("Could not parse filename, %s", fileName)
	}

	return data.Version(1), data.Sequence(currentSequence), nil
}
