package pubsubsink

import (
	"context"
	"errors"
	"testing"

	"cloud.google.com/go/pubsub/v2"
	"github.com/arquivei/goduck/pipeline"
	"github.com/stretchr/testify/assert"
)

func TestSink_Store(t *testing.T) {
	type args struct {
		ctx      context.Context
		messages []pipeline.SinkMessage
	}
	tests := []struct {
		name      string
		setupMock func(m *MockPubsubClientGateway)
		args      args
		wantErr   bool
	}{
		{
			name: "success - no messages",
			setupMock: func(m *MockPubsubClientGateway) {
				topic1 := newMockTopicGateway(t)
				m.EXPECT().Topic("topic1").Return(topic1).Once()

				topic1.EXPECT().Exists(context.Background()).Return(true, nil).Once()
				topic1.EXPECT().Stop().Once()

				result1 := newMockPublishResult(t)
				topic1.EXPECT().Publish(context.Background(), &pubsub.Message{
					Data: []byte("message1"),
				}).Return(result1).Once()
				result1.EXPECT().Get(context.Background()).Return("message1", nil).Once()
			},
			args: args{
				ctx: context.Background(),
				messages: []pipeline.SinkMessage{
					SinkMessage{
						Topic: "topic1",
						Msg:   []byte("message1"),
					},
				},
			},
			wantErr: false,
		},
		{
			name: "success - MUlti messages with same topic",
			setupMock: func(m *MockPubsubClientGateway) {
				topic1 := newMockTopicGateway(t)
				m.EXPECT().Topic("topic1").Return(topic1).Once()

				topic1.EXPECT().Exists(context.Background()).Return(true, nil).Once()
				topic1.EXPECT().Stop().Once()

				result1 := newMockPublishResult(t)
				topic1.EXPECT().Publish(context.Background(), &pubsub.Message{
					Data: []byte("message1"),
				}).Return(result1).Once()
				result1.EXPECT().Get(context.Background()).Return("message1", nil).Once()

				result2 := newMockPublishResult(t)
				topic1.EXPECT().Publish(context.Background(), &pubsub.Message{
					Data: []byte("message2"),
				}).Return(result2).Once()
				result2.EXPECT().Get(context.Background()).Return("message2", nil).Once()
			},
			args: args{
				ctx: context.Background(),
				messages: []pipeline.SinkMessage{
					SinkMessage{
						Topic: "topic1",
						Msg:   []byte("message1"),
					},
					SinkMessage{
						Topic: "topic1",
						Msg:   []byte("message2"),
					},
				},
			},
			wantErr: false,
		},
		{
			name: "success - MUlti messages with multiple topics",
			setupMock: func(m *MockPubsubClientGateway) {
				topic1 := newMockTopicGateway(t)
				m.EXPECT().Topic("topic1").Return(topic1).Once()

				topic1.EXPECT().Exists(context.Background()).Return(true, nil).Once()
				topic1.EXPECT().Stop().Once()

				result1 := newMockPublishResult(t)
				topic1.EXPECT().Publish(context.Background(), &pubsub.Message{
					Data: []byte("message1"),
				}).Return(result1).Once()
				result1.EXPECT().Get(context.Background()).Return("message1", nil).Once()

				result2 := newMockPublishResult(t)
				topic1.EXPECT().Publish(context.Background(), &pubsub.Message{
					Data: []byte("message2"),
				}).Return(result2).Once()
				result2.EXPECT().Get(context.Background()).Return("message2", nil).Once()

				topic2 := newMockTopicGateway(t)
				m.EXPECT().Topic("topic2").Return(topic2).Once()
				topic2.EXPECT().Exists(context.Background()).Return(true, nil).Once()
				topic2.EXPECT().Stop().Once()

				result3 := newMockPublishResult(t)
				topic2.EXPECT().Publish(context.Background(), &pubsub.Message{
					Data: []byte("message3"),
				}).Return(result3).Once()
				result3.EXPECT().Get(context.Background()).Return("message3", nil).Once()

			},
			args: args{
				ctx: context.Background(),
				messages: []pipeline.SinkMessage{
					SinkMessage{
						Topic: "topic1",
						Msg:   []byte("message1"),
					},
					SinkMessage{
						Topic: "topic2",
						Msg:   []byte("message3"),
					},
					SinkMessage{
						Topic: "topic1",
						Msg:   []byte("message2"),
					},
				},
			},
			wantErr: false,
		},
		{
			name: "success - no messages",
			setupMock: func(m *MockPubsubClientGateway) {
			},
			args: args{
				ctx:      context.Background(),
				messages: []pipeline.SinkMessage{},
			},
			wantErr: false,
		},
		{
			name: "error - topic does not exist",
			setupMock: func(m *MockPubsubClientGateway) {
				topic1 := newMockTopicGateway(t)
				m.EXPECT().Topic("topic1").Return(topic1).Once()

				topic1.EXPECT().Exists(context.Background()).Return(false, nil).Once()
			},
			args: args{
				ctx: context.Background(),
				messages: []pipeline.SinkMessage{
					SinkMessage{
						Topic: "topic1",
						Msg:   []byte("message1"),
					},
				},
			},
			wantErr: true,
		},
		{
			name: "error - topic exists check fails",
			setupMock: func(m *MockPubsubClientGateway) {
				topic1 := newMockTopicGateway(t)
				m.EXPECT().Topic("topic1").Return(topic1).Once()

				topic1.EXPECT().Exists(context.Background()).Return(false, errors.New("error")).Once()
			},
			args: args{
				ctx: context.Background(),
				messages: []pipeline.SinkMessage{
					SinkMessage{
						Topic: "topic1",
						Msg:   []byte("message1"),
					},
				},
			},
			wantErr: true,
		},
		{
			name: "error - publish fails",
			setupMock: func(m *MockPubsubClientGateway) {
				topic1 := newMockTopicGateway(t)
				m.EXPECT().Topic("topic1").Return(topic1).Once()

				topic1.EXPECT().Exists(context.Background()).Return(true, nil).Once()
				topic1.EXPECT().Stop().Once()

				result1 := newMockPublishResult(t)
				topic1.EXPECT().Publish(context.Background(), &pubsub.Message{
					Data: []byte("message1"),
				}).Return(result1).Once()
				result1.EXPECT().Get(context.Background()).Return("message1", errors.New("error")).Once()
			},
			args: args{
				ctx: context.Background(),
				messages: []pipeline.SinkMessage{
					SinkMessage{
						Topic: "topic1",
						Msg:   []byte("message1"),
					},
				},
			},
			wantErr: true,
		},
		{
			name: "error - invalid message type",
			setupMock: func(m *MockPubsubClientGateway) {
			},
			args: args{
				ctx: context.Background(),
				messages: []pipeline.SinkMessage{
					"bad msg type",
				},
			},
			wantErr: true,
		},
		{
			name: "error - empty topic",
			setupMock: func(m *MockPubsubClientGateway) {
			},
			args: args{
				ctx: context.Background(),
				messages: []pipeline.SinkMessage{
					SinkMessage{
						Topic: "",
						Msg:   []byte("message1"),
					},
				},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gateway := NewMockPubsubClientGateway(t)
			if tt.setupMock != nil {
				tt.setupMock(gateway)
			}
			s := &Sink{
				pubsubClient: gateway,
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
		getGateway func(t *testing.T) PubsubClientGateway
		wantPanic  bool
	}{
		{
			name: "success",
			getGateway: func(t *testing.T) PubsubClientGateway {
				gateway := NewMockPubsubClientGateway(t)
				gateway.EXPECT().Close().Return(nil).Times(1)
				return gateway
			},
			wantPanic: false,
		},
		{
			name: "panics - nil gateway",
			getGateway: func(t *testing.T) PubsubClientGateway {
				return nil
			},
			wantPanic: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gateway := tt.getGateway(t)
			if tt.wantPanic {
				assert.Panics(t, func() {
					MustNew(gateway)
				})
				return
			}
			assert.NotPanics(t, func() {
				_, closeFunc := MustNew(gateway)
				closeFunc()
			})
		})
	}
}
