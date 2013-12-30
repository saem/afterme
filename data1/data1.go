package data1

import (
	"fmt"
	"github.com/saem/afterme/data"
	"os"
	"regexp"
	"strconv"
)

// Versioned log file name parsing and validation

// Data Structures to support version 1 file format
type dataFile struct {
	version          data.Version
	startingSequence data.Sequence
	dataDir          string
	file             *os.File
}

func NewDataFile(startingSequence data.Sequence, dataDir string) (df *dataFile) {
	df = new(dataFile)
	df.version = data.Version(1)
	df.startingSequence = startingSequence
	df.dataDir = dataDir

	return df
}

func (df dataFile) CreateForWrite() (err error) {
	if df.file != nil {
		return data.DataFileError{df.Name(), data.ALREADY_OPEN}
	}
	df.file, err = os.OpenFile(df.fullName(), os.O_APPEND|os.O_CREATE|os.O_EXCL, 0644)

	return err
}

func (df dataFile) OpenForRead() (err error) {
	if df.file != nil {
		return data.DataFileError{df.Name(), data.ALREADY_OPEN}
	}
	df.file, err = os.OpenFile(df.fullName(), os.O_RDONLY, 0644)

	return err
}

func (df dataFile) Write(message data.Message) (err error) {
	header, body, err := message.Marshal()
	if err != nil {
		return err
	}

	// TODO: properly handle write errors

	_, err = df.file.Write([]byte(header))
	if err != nil {
		return err
	}

	_, err = df.file.Write(body)
	if err != nil {
		return err
	}

	return nil
}

func (df dataFile) Close() (err error) {
	err = nil
	if df.file != nil {
		err = df.file.Close()
	}

	df.file = nil //we only allow reading XOR writing, clearing so we can use it as a mutex

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

type Message struct {
	Sequence    data.Sequence
	TimeStamp   int64
	MessageSize uint32
	//@todo add the integrity hash
	Body []byte
}

func (message Message) Marshal() (header string, body []byte, err error) {
	header = fmt.Sprintf("%d-%d-%d\n", message.Sequence, message.TimeStamp, message.MessageSize)
	return header, message.Body, nil
}
