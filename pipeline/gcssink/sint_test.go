package gcssink

import (
	"context"
	"testing"

	"cloud.google.com/go/storage"
	"github.com/arquivei/foundationkit/errors"
	"github.com/arquivei/goduck/pipeline"
	"github.com/stretchr/testify/assert"
	mock "github.com/stretchr/testify/mock"
)

func TestMustNewParallel(t *testing.T) {
	t.Parallel()

	type args struct {
		clientGateway func(t *testing.T) GcsClientGateway
		contentType   string
		chunkSize     int
		retrierConfig []storage.RetryOption
	}

	tests := []struct {
		name      string
		args      args
		wantPanic bool
	}{
		{
			name: "[SUCCESS] - should not panic when args are valid",
			args: args{
				clientGateway: func(t *testing.T) GcsClientGateway {
					mockGateway := NewMockGcsClientGateway(t)
					mockGateway.EXPECT().Close().Return(nil).Once()
					return mockGateway
				},
				contentType:   "application/json",
				chunkSize:     100,
				retrierConfig: []storage.RetryOption{},
			},
			wantPanic: false,
		},

		{
			name: "[PANIC] - should panic when clientGateway is nil",
			args: args{
				clientGateway: func(t *testing.T) GcsClientGateway {
					return nil
				},
				contentType:   "application/json",
				chunkSize:     100,
				retrierConfig: []storage.RetryOption{},
			},
			wantPanic: true,
		},

		{
			name: "[PANIC] - should panic when contentType is empty",
			args: args{
				clientGateway: func(t *testing.T) GcsClientGateway {
					mockGateway := NewMockGcsClientGateway(t)
					return mockGateway
				},
				contentType:   "",
				chunkSize:     100,
				retrierConfig: []storage.RetryOption{},
			},
			wantPanic: true,
		},

		{
			name: "[PANIC] - should panic when chunkSize is zero",
			args: args{
				clientGateway: func(t *testing.T) GcsClientGateway {
					mockGateway := NewMockGcsClientGateway(t)
					return mockGateway
				},
				contentType:   "application/json",
				chunkSize:     0,
				retrierConfig: []storage.RetryOption{},
			},
			wantPanic: true,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			gateway := test.args.clientGateway(t)
			if test.wantPanic {
				assert.Panics(t, func() {
					MustNewParallel(gateway, test.args.contentType, test.args.chunkSize, test.args.retrierConfig)
				})
				return
			}
			assert.NotPanics(t, func() {
				_, closeFunc := MustNewParallel(gateway, test.args.contentType, test.args.chunkSize, test.args.retrierConfig)
				closeFunc()
			})
		})
	}
}

func TestStore(t *testing.T) {
	t.Parallel()

	type args struct {
		setupMock func(m *MockGcsClientGateway)
		context   context.Context
		messages  []pipeline.SinkMessage
	}

	tests := []struct {
		name          string
		args          args
		err           bool
		expectedError string
	}{
		{
			name: "[SUCCESS] - should return no error when messages are successfully stored",
			args: args{
				setupMock: func(m *MockGcsClientGateway) {
					m.EXPECT().GetWriter(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Times(2)
					m.EXPECT().Write(mock.Anything, mock.Anything).Return(nil).Times(2)
					m.EXPECT().Close().Return(nil).Times(1)
				},
				context: context.Background(),
				messages: []pipeline.SinkMessage{
					SinkMessage{
						Data:        []byte("test"),
						StoragePath: "storage/path",
						Bucket:      "bucket",
						Metadata: map[string]string{
							"key": "value",
						},
					},
					SinkMessage{
						Data:        []byte("test1"),
						StoragePath: "storage/path1",
						Bucket:      "bucket1",
						Metadata: map[string]string{
							"key1": "value1",
						},
					},
				},
			},
			err: false,
		},
		{
			name: "[SUCCESS] - should return no error when message is nil",
			args: args{
				setupMock: func(m *MockGcsClientGateway) {
					m.EXPECT().Close().Return(nil).Times(1)
				},
				context:  context.Background(),
				messages: nil,
			},
			err: false,
		},

		{
			name: "[SUCCESS] - should return no error when message is empty",
			args: args{
				setupMock: func(m *MockGcsClientGateway) {
					m.EXPECT().Close().Return(nil).Times(1)
				},
				context:  context.Background(),
				messages: []pipeline.SinkMessage{},
			},
			err: false,
		},

		{
			name: "[ERROR] - should return error when message is invalid",
			args: args{
				setupMock: func(m *MockGcsClientGateway) {
					m.EXPECT().Close().Return(nil).Times(1)
				},
				context: context.Background(),
				messages: []pipeline.SinkMessage{
					"bad msg type",
				},
			},
			err:           true,
			expectedError: errors.E(errors.Op("gcssink.gcsParallelWriter.Store"), ErrFailedToStoreMessages, errors.KV("errors", []error{errors.E(ErrInvalidSinkMessage, errors.KV("type", "string"))})).Error(),
		},

		{
			name: "[ERROR] - should return error when message data is empty",
			args: args{
				setupMock: func(m *MockGcsClientGateway) {
					m.EXPECT().GetWriter(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Times(1)
					m.EXPECT().Write(mock.Anything, mock.Anything).Return(errors.E(errors.Op("gcssink.gcsClientGateway.Write"), ErrInvalidSinkMessage)).Times(1)
					m.EXPECT().Close().Return(nil).Times(1)
				},
				context: context.Background(),
				messages: []pipeline.SinkMessage{
					SinkMessage{
						Data:        nil,
						StoragePath: "storage/path",
						Bucket:      "bucket",
						Metadata: map[string]string{
							"key": "value",
						},
					},
				},
			},
			err:           true,
			expectedError: errors.E(errors.Op("gcssink.gcsParallelWriter.Store"), ErrFailedToStoreMessages, errors.KV("errors", []error{errors.E(errors.Op("gcssink.gcsClientGateway.Write"), ErrInvalidSinkMessage)})).Error(),
		},

		{
			name: "[ERROR] - should return error when writer fails to write",
			args: args{
				setupMock: func(m *MockGcsClientGateway) {
					m.EXPECT().GetWriter(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Times(1)
					m.EXPECT().Write(mock.Anything, mock.Anything).Return(errors.E(errors.Op("gcssink.gcsClientGateway.Write"), ErrFailedToWriteAtBucket, errors.KV("bucket", "bucket"), errors.KV("path", "path"), errors.KV("error", storage.ErrObjectNotExist))).Times(1)
					m.EXPECT().Close().Return(nil).Times(1)
				},
				context: context.Background(),
				messages: []pipeline.SinkMessage{
					SinkMessage{
						Data:        []byte("test"),
						StoragePath: "path",
						Bucket:      "bucket",
						Metadata: map[string]string{
							"key": "value",
						},
					},
				},
			},
			err:           true,
			expectedError: errors.E(errors.Op("gcssink.gcsParallelWriter.Store"), ErrFailedToStoreMessages, errors.KV("errors", []error{errors.E(errors.Op("gcssink.gcsClientGateway.Write"), ErrFailedToWriteAtBucket, errors.KV("bucket", "bucket"), errors.KV("path", "path"), errors.KV("error", storage.ErrObjectNotExist))})).Error(),
		},

		{
			name: "[ERROR] - should return error when writer fails to close",
			args: args{
				setupMock: func(m *MockGcsClientGateway) {
					m.EXPECT().GetWriter(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Times(1)
					m.EXPECT().Write(mock.Anything, mock.Anything).Return(errors.E(errors.Op("gcssink.gcsClientGateway.Write"), ErrFailedToCloseBucket, errors.KV("bucket", "bucket"), errors.KV("path", "path"), errors.KV("error", storage.ErrObjectNotExist))).Times(1)
					m.EXPECT().Close().Return(nil).Times(1)
				},
				context: context.Background(),
				messages: []pipeline.SinkMessage{
					SinkMessage{
						Data:        []byte("test"),
						StoragePath: "path",
						Bucket:      "bucket",
						Metadata: map[string]string{
							"key": "value",
						},
					},
				},
			},
			err:           true,
			expectedError: errors.E(errors.Op("gcssink.gcsParallelWriter.Store"), ErrFailedToStoreMessages, errors.KV("errors", []error{errors.E(errors.Op("gcssink.gcsClientGateway.Write"), ErrFailedToCloseBucket, errors.KV("bucket", "bucket"), errors.KV("path", "path"), errors.KV("error", storage.ErrObjectNotExist))})).Error(),
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			assert.NotPanics(t, func() {
				gateway := NewMockGcsClientGateway(t)

				if test.args.setupMock != nil {
					test.args.setupMock(gateway)
				}

				sink, closeFn := MustNewParallel(gateway, "application/json", 100, []storage.RetryOption{})
				defer closeFn()

				err := sink.Store(test.args.context, test.args.messages...)

				if test.expectedError == "" {
					assert.NoError(t, err)
				} else {
					assert.EqualError(t, err, test.expectedError)
					assert.Equal(t, test.expectedError, err.Error())
				}
			})
		})
	}
}
