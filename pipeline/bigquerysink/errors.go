package bigquerysink

import "errors"

var (
	// ErrUnkownMessageType is returned when the sink message
	// received is not of the type Message.
	ErrUnkownMessageType = errors.New("unkown message type")

	// ErrEmptyProjectID is returned when the Message.ProjectID is empty.
	ErrEmptyProjectID = errors.New("project id is missing from the message")

	// ErrEmptyDatasetID is returned when the Message.DatasetID is empty.
	ErrEmptyDatasetID = errors.New("dataset id is missing from the message")

	// ErrEmptyTableID is returned when the Message.TableID is empty.
	ErrEmptyTableID = errors.New("table id is missing from the message")

	// ErrEmptyData is returned when the Message.Data is nil.
	ErrEmptyData = errors.New("data is missing from the message")
)
