package elasticsink

type Option func(*elasticSink)

func WithIgnoreIndexNotFoundOnDelete() Option {
	return func(es *elasticSink) {
		es.deleteOptions.IgnoreIndexNotFoundError = true
	}
}
