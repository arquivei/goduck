package elasticsink

import (
	"context"

	"github.com/arquivei/goduck/pipeline"

	"github.com/arquivei/foundationkit/errors"
	"github.com/olivere/elastic/v7"
	"github.com/rs/zerolog/log"
)

// SinkMessage is the input for the Elastic Sink
// Deprecated: this is an alias to IndexMessage, use that instead.
type SinkMessage = IndexMessage

// IndexMessage saves the document into elasticsearch
type IndexMessage struct {
	ID       string
	Index    string
	Document interface{}
}

// DeleteMessage deletes the document from the elasticsearch
type DeleteMessage struct {
	ID    string
	Index string
}

type elasticSink struct {
	client *elastic.Client
}

// MustNew creates a new sink that saves documents to elastic
func MustNew(
	client *elastic.Client,
) pipeline.Sink {
	if client == nil {
		panic("elasticsearch client is nil")
	}
	return &elasticSink{
		client: client,
	}
}

func (e *elasticSink) Store(ctx context.Context, input ...pipeline.SinkMessage) error {
	const op = errors.Op("elasticsink.elasticSink.Store")

	if len(input) == 0 {
		return nil
	}

	bulkRequest := e.client.Bulk()
	for _, message := range input {
		switch sinkMessage := message.(type) {
		case IndexMessage:
			item, err := newIndexBulkItem(sinkMessage)
			if err != nil {
				return errors.E(op, err)
			}
			bulkRequest.Add(item)
		case DeleteMessage:
			item, err := newDeleteBulkItem(sinkMessage)
			if err != nil {
				return errors.E(op, err)
			}
			bulkRequest.Add(item)
		default:
			return errors.E(op, "message should have type elasticsink.SinkMessage", errors.SeverityInput)
		}
	}

	response, err := bulkRequest.Do(ctx)
	if err != nil {
		return errors.E(op, err, errors.SeverityRuntime)
	}
	if response.Errors {
		logInsertFailed(ctx, response)
		return errors.E(op, "some items failed to index", errors.SeverityRuntime)
	}
	return nil
}

func newIndexBulkItem(sinkMessage IndexMessage) (elastic.BulkableRequest, error) {
	const op errors.Op = "newIndexBulkItem"

	if sinkMessage.ID == "" {
		return nil, errors.E(op, "mandatory ID", errors.SeverityInput)
	}
	if sinkMessage.Index == "" {
		return nil, errors.E(op, "mandatory Index", errors.SeverityInput)
	}
	if sinkMessage.Document == nil {
		return nil, errors.E(op, "mandatory Document", errors.SeverityInput)
	}

	return elastic.
		NewBulkIndexRequest().
		Index(sinkMessage.Index).
		Id(sinkMessage.ID).
		Doc(sinkMessage.Document), nil
}

func newDeleteBulkItem(sinkMessage DeleteMessage) (elastic.BulkableRequest, error) {
	const op errors.Op = "newDeleteBulkItem"

	if sinkMessage.ID == "" {
		return nil, errors.E(op, "mandatory ID", errors.SeverityInput)
	}
	if sinkMessage.Index == "" {
		return nil, errors.E(op, "mandatory Index", errors.SeverityInput)
	}

	return elastic.NewBulkDeleteRequest().
		Index(sinkMessage.Index).
		Id(sinkMessage.ID), nil
}

func logInsertFailed(ctx context.Context, response *elastic.BulkResponse) {
	logger := log.Ctx(ctx)
	for _, item := range response.Failed() {
		logEvent := logger.Error().
			Str("elastic_index", item.Index).
			Str("elastic_id", item.Id)

		if item.Error != nil {
			logEvent = logEvent.
				Str("error_type", item.Error.Type).
				Str("error_message", item.Error.Reason)
		}

		logEvent.Msg("Failed to index elastic document")
	}
}
