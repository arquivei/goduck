package main

// This is a simple example showing all possible options for executing a bigquery sink.

import (
	"context"
	"flag"
	"os"
	"time"

	"github.com/arquivei/goduck/pipeline/bigquerysink"

	"cloud.google.com/go/bigquery"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

type data struct {
	Key       string
	Value     string
	CreatedAt time.Time
}

func main() {
	projectID := flag.String("projectid", "", "Project ID.")
	dataset := flag.String("dataset", "", "Dataset name.")
	table := flag.String("table", "", "Table name.")
	key := flag.String("key", "", "Key.")
	value := flag.String("value", "", "Value.")

	flag.Parse()

	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	zerolog.SetGlobalLevel(zerolog.TraceLevel)

	if *key == "" {
		log.Fatal().Msg("key must be provided")

	}

	ctx := context.Background()
	ctx = log.Logger.WithContext(ctx)

	client, err := bigquery.NewClient(ctx, *projectID)
	if err != nil {
		log.Fatal().Err(err).Send()
	}

	options := []bigquerysink.Option{
		bigquerysink.WithLogLevel(zerolog.TraceLevel),
		bigquerysink.WithIgnoreUnkownMessageTypes(),
		bigquerysink.WithIgnoreValidationErrors(),
	}

	sink := bigquerysink.MustNew(client, options...)

	m := bigquerysink.Message{
		ProjectID: *projectID,
		DatasetID: *dataset,
		TableID:   *table,
		Data: &data{
			Key:       *key,
			Value:     *value,
			CreatedAt: time.Now(),
		},
	}

	// This will be ignored because it has empty data
	m2 := bigquerysink.Message{
		ProjectID: *projectID,
		DatasetID: *dataset,
		TableID:   *table,
	}

	// This will be ignored because it's not a supported message type.
	m3 := struct{}{}

	err = sink.Store(ctx, m, m2, m3)
	if err != nil {
		log.Fatal().Err(err).Send()
	}
	log.Info().Msg("Message stored in bigquery")
}
