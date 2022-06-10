package elasticsink

import (
	"context"

	"github.com/arquivei/goduck/pipeline"

	"github.com/arquivei/foundationkit/errors"
	"github.com/olivere/elastic/v7"
)

const (
	actionDelete                    = "delete"
	errorTypeIndexNotFoundException = "index_not_found_exception"
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

	deleteOptions struct {
		IgnoreIndexNotFoundError bool
	}
}

// MustNew creates a new sink that saves documents to elastic
func MustNew(
	client *elastic.Client,
	options ...Option,
) pipeline.Sink {
	if client == nil {
		panic("elasticsearch client is nil")
	}

	es := &elasticSink{
		client: client,
	}

	for _, opt := range options {
		opt(es)
	}
	return es
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
		// One or more errors happened, but because we could
		// be ignoring some of them we must check the errors.
		// Let's extract the errors that are not ignored and
		// return a new error with only those errors. If no
		// error is returned here, than just return success.
		errs := e.extractErrorsFromBulkItems(response)
		if len(errs) > 0 {
			return errors.E(op, "some items failed to be stored", errors.SeverityRuntime, errors.KV("errors", errs))
		}
	}
	return nil
}

func (e *elasticSink) shouldIgnoreError(action string, result *elastic.BulkResponseItem) bool {
	return action == actionDelete &&
		e.deleteOptions.IgnoreIndexNotFoundError &&
		result.Error != nil &&
		result.Error.Type == errorTypeIndexNotFoundException
}

func (e *elasticSink) extractErrorsFromBulkItems(response *elastic.BulkResponse) []string {
	errs := []string{}
	for _, item := range response.Items {
		for action, result := range item {
			if bulkItemSucceeded(result) {
				continue
			}
			if e.shouldIgnoreError(action, result) {
				continue
			}

			reason := "error message not available"
			if result.Error != nil {
				reason = result.Error.Type + ": " + result.Error.Reason
			}
			errs = append(errs, action+" "+result.Index+"/"+result.Id+":"+reason)
		}
	}
	return errs
}

func bulkItemSucceeded(result *elastic.BulkResponseItem) bool {
	return result.Status >= 200 && result.Status <= 299
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
