package pubsubsink

import (
	"context"
	"fmt"
	"testing"

	"cloud.google.com/go/pubsub"
	"github.com/arquivei/goduck/pipeline"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestSink_Store(t *testing.T) {
	type args struct {
		ctx      context.Context
		messages []pipeline.SinkMessage
	}
	tests := []struct {
		name      string
		setupMock func(m *MockTopicGateway)
		args      args
		wantErr   bool
	}{
		{
			name: "success - no messages",
			setupMock: func(m *MockTopicGateway) {
			},
			args: args{
				ctx:      context.Background(),
				messages: []pipeline.SinkMessage{},
			},
			wantErr: false,
		},
		{
			name: "success - single message",
			setupMock: func(m *MockTopicGateway) {
				result := NewMockPublishResult(t)
				result.EXPECT().Get(mock.Anything).Return("id", nil).Once()

				m.EXPECT().Publish(context.Background(), &pubsub.Message{
					Data: []byte("test"),
				}).Return(result).Once()
			},
			args: args{
				ctx: context.Background(),
				messages: []pipeline.SinkMessage{
					SinkMessage{
						[]byte("test"),
					},
				},
			},
			wantErr: false,
		},
		{
			name: "success - multiple messages",
			setupMock: func(m *MockTopicGateway) {
				result := NewMockPublishResult(t)
				result.EXPECT().Get(mock.Anything).Return("id", nil).Once()
				result2 := NewMockPublishResult(t)
				result2.EXPECT().Get(mock.Anything).Return("id", nil).Once()

				m.EXPECT().Publish(context.Background(), &pubsub.Message{
					Data: []byte("test"),
				}).Return(result).Once()
				m.EXPECT().Publish(context.Background(), &pubsub.Message{
					Data: []byte("test2"),
				}).Return(result2).Once()
			},
			args: args{
				ctx: context.Background(),
				messages: []pipeline.SinkMessage{
					SinkMessage{
						[]byte("test"),
					},
					SinkMessage{
						[]byte("test2"),
					},
				},
			},
			wantErr: false,
		},
		{
			name: "error - publish error",
			setupMock: func(m *MockTopicGateway) {
				result := NewMockPublishResult(t)
				result.EXPECT().Get(mock.Anything).Return("", fmt.Errorf("error")).Once()

				m.EXPECT().Publish(context.Background(), &pubsub.Message{
					Data: []byte("test"),
				}).Return(result).Once()
			},
			args: args{
				ctx: context.Background(),
				messages: []pipeline.SinkMessage{
					SinkMessage{
						[]byte("test"),
					},
				},
			},
			wantErr: true,
		},
		{
			name:      "error - unexpected message type",
			setupMock: func(m *MockTopicGateway) {},
			args: args{
				ctx: context.Background(),
				messages: []pipeline.SinkMessage{
					"unexpected_message",
				},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			gateway := NewMockTopicGateway(t)
			if tt.setupMock != nil {
				tt.setupMock(gateway)
			}
			s := &Sink{
				topicGateway: gateway,
			}
			if err := s.Store(tt.args.ctx, tt.args.messages...); (err != nil) != tt.wantErr {
				t.Errorf("Sink.Store() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestMustNew(t *testing.T) {
	tests := []struct {
		name       string
		getGateway func(t *testing.T) TopicGateway
		wantPanic  bool
	}{
		{
			name: "success",
			getGateway: func(t *testing.T) TopicGateway {
				gateway := NewMockTopicGateway(t)
				gateway.EXPECT().Exists(context.Background()).Return(true, nil).Once()
				gateway.EXPECT().Stop().Once()
				return gateway
			},
			wantPanic: false,
		},
		{
			name: "panics - nil gateway",
			getGateway: func(t *testing.T) TopicGateway {
				return nil
			},
			wantPanic: true,
		},
		{
			name: "panics - topic doesn't exist",
			getGateway: func(t *testing.T) TopicGateway {
				gateway := NewMockTopicGateway(t)
				gateway.EXPECT().Exists(context.Background()).Return(false, nil).Once()
				return gateway
			},
			wantPanic: true,
		},
		{
			name: "panics - error checking if topic exists",
			getGateway: func(t *testing.T) TopicGateway {
				gateway := NewMockTopicGateway(t)
				gateway.EXPECT().Exists(context.Background()).Return(true, fmt.Errorf("err")).Once()
				return gateway
			},
			wantPanic: true,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			gateway := tt.getGateway(t)
			fmt.Println(tt.name, gateway)
			if tt.wantPanic {
				assert.Panics(t, func() {
					fmt.Println(tt.name, gateway)
					MustNew(gateway)
				})
				return
			}
			assert.NotPanics(t, func() {
				fmt.Println(tt.name, gateway)
				_, closeFunc := MustNew(gateway)
				closeFunc()
			})
		})
	}
}
