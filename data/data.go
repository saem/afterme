package data

import (
	"fmt"
)

// Core data types, used in read/writing, and operational observation
type Sequence uint64
type Version uint64
type DataFile interface {
	CreateForWrite() error
	OpenForRead() error
	Close() error
	Name() string
}
type Message interface {
	Marshal() (header string, body []byte, err error)
}

// Errors

type DataFileErrorCode int
const (
	ALREADY_OPEN DataFileErrorCode = 1 << iota
	ALREADY_CREATED
	FILE_CLOSED
	NO_FILES_FOUND
)
type DataFileError struct {
	Name string
	Code DataFileErrorCode
}
func (e DataFileError) Error() string {
	var fmtStr = ""
	switch {
	case e.Code == ALREADY_OPEN:
		fmtStr = "File (%s) already open"
	case e.Code == ALREADY_CREATED:
		fmtStr = "File (%s) already created"
	case e.Code == FILE_CLOSED:
		fmtStr = "File (%s) closed"
	}

	return fmt.Sprintf(fmtStr, e.Name)
} 
