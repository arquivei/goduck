package bigquerysink

import (
	"context"

	"cloud.google.com/go/bigquery"
	"github.com/arquivei/foundationkit/errors"
	"github.com/arquivei/goduck/pipeline"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

type bigquerySink struct {
	client *bigquery.Client

	logLevel                     zerolog.Level
	shouldIgnoreUnkownMessages   bool
	shouldIgnoreValidationErrors bool
}

// Message is the pipeline message being stored.
type Message struct {
	// TableID is the table name where the Data will be saved.
	TableID string
	// DatasetID is the dataset containing the Table.
	DatasetID string
	// ProjectID is the GCP's project Id which contains the dataset.
	ProjectID string

	// Data is the data being saved. It should be a type compatible with bigquery package.
	// This means that types as `map` or  `*string` will fail.
	// Read the bigquery documentation for more details: https://pkg.go.dev/cloud.google.com/go/bigquery.
	Data interface{}
}

// MustNew returns a new Sink that saves data to bigquiery.
func MustNew(client *bigquery.Client, options ...Option) pipeline.Sink {
	if client == nil {
		panic("bigquery client is nil")
	}
	s := &bigquerySink{
		client:   client,
		logLevel: zerolog.Disabled,
	}

	for _, opt := range options {
		opt(s)
	}

	return s
}

// Store saves all the sink messages into their respective tables. It expects all messages to
// be of the type Message.
func (s *bigquerySink) Store(ctx context.Context, sinkmessages ...pipeline.SinkMessage) error {
	const op errors.Op = "bigquerysink.bigquerySink.Store"
	logger := log.Ctx(ctx).Level(s.logLevel)

	// Messages are grouped by table and sent together.
	putJobs := make(map[string]*putJob)

	for _, sinkmessage := range sinkmessages {
		message, ok := sinkmessage.(Message)
		if !ok {
			if s.shouldIgnoreUnkownMessages {
				logger.Warn().Msg("[bigquerySink] Ignoring unkown message type.")
				continue
			}
			return errors.E(op, ErrUnkownMessageType)
		}

		err := checkMessage(message)
		if err != nil {
			if s.shouldIgnoreValidationErrors {
				logger.Warn().Msg("[bigquerySink] Ignoring invalid messages.")
				continue
			}
			return errors.E(op, errors.SeverityInput, err)
		}

		t := s.getTable(message)
		tname := t.FullyQualifiedName()
		job := putJobs[tname]
		if job == nil {
			job = &putJob{
				table: t,
				rows:  make([]interface{}, 0, len(sinkmessages)),
			}
			putJobs[tname] = job
		}

		job.rows = append(job.rows, message.Data)
		logger.Trace().
			Str("bigquery_table", tname).
			Msg("[bigquerySink] Message processed.")
	}

	for _, job := range putJobs {
		err := job.Execute(ctx)
		if err != nil {
			return errors.E(op, err, errors.KV("table", job.table.FullyQualifiedName()))
		}
	}

	logger.Trace().Msg("[bigquerySink] All messages processed.")

	return nil
}

func (s *bigquerySink) logger(ctx context.Context) zerolog.Logger {
	if s.logLevel == zerolog.Disabled {
		return zerolog.Nop()
	}

	return log.Ctx(ctx).Level(s.logLevel)
}

func (s *bigquerySink) getTable(m Message) *bigquery.Table {
	return s.client.DatasetInProject(m.ProjectID, m.DatasetID).Table(m.TableID)
}

func checkMessage(m Message) error {
	const op errors.Op = "checkMessage"

	if m.ProjectID == "" {
		return errors.E(op, ErrEmptyProjectID)
	}
	if m.DatasetID == "" {
		return errors.E(op, ErrEmptyDatasetID)
	}

	if m.TableID == "" {
		return errors.E(op, ErrEmptyTableID)
	}

	if m.Data == nil {
		return errors.E(op, ErrEmptyData)
	}

	return nil
}

// putJob aggregates rows to be inserted together
type putJob struct {
	table *bigquery.Table
	rows  []interface{}
}

// Execute inserts all the rows of the putJob into
// the table.
func (j *putJob) Execute(ctx context.Context) error {
	return j.table.Inserter().Put(ctx, j.rows)
}
