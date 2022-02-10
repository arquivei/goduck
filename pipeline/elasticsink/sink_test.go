package elasticsink

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/arquivei/goduck/pipeline"

	"github.com/arquivei/foundationkit/errors"
	"github.com/olivere/elastic/v7"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestAddBulk(t *testing.T) {
	tests := []struct {
		name          string
		httpHandler   *mockServer
		input         []pipeline.SinkMessage
		expectedError string
	}{
		{
			name: "Success - Simple",
			httpHandler: func() *mockServer {
				server := new(mockServer)
				server.On(
					"ServeHTTP",
					"/_bulk",
					`{"index":{"_id":"ID1","_index":"index1"}}
{"a":2,"b":3}
{"index":{"_id":"ID2","_index":"index1"}}
{"a":4,"b":4}
{"index":{"_id":"ID3","_index":"index2"}}
{"a":5,"b":6}`,
				).Once().Return(
					`{"took":80,"errors":false,"items":[{"index":{"_index":"index1","_type":"_doc","_id":"ID1","_version":1,"result":"created","_shards":{"total":2,"successful":1,"failed":0},"_seq_no":0,"_primary_term":1,"status":201}},{"index":{"_index":"index1","_type":"_doc","_id":"ID2","_version":1,"result":"created","_shards":{"total":2,"successful":1,"failed":0},"_seq_no":1,"_primary_term":1,"status":201}},{"index":{"_index":"index2","_type":"_doc","_id":"ID3","_version":1,"result":"created","_shards":{"total":2,"successful":1,"failed":0},"_seq_no":0,"_primary_term":1,"status":201}}]}`,
					201,
				)

				return server
			}(),
			input: []pipeline.SinkMessage{
				IndexMessage{
					ID:    "ID1",
					Index: "index1",
					Document: map[string]interface{}{
						"a": 2,
						"b": 3,
					},
				},
				IndexMessage{
					ID:    "ID2",
					Index: "index1",
					Document: map[string]interface{}{
						"a": 4,
						"b": 4,
					},
				},
				IndexMessage{
					ID:    "ID3",
					Index: "index2",
					Document: map[string]interface{}{
						"a": 5,
						"b": 6,
					},
				},
			},
			expectedError: "",
		},
		{
			name: "Success - Delete",
			httpHandler: func() *mockServer {
				server := new(mockServer)
				server.On(
					"ServeHTTP",
					"/_bulk",
					`{"delete":{"_id":"ID1","_index":"index1"}}
{"delete":{"_id":"ID2","_index":"index1"}}
{"delete":{"_id":"ID3","_index":"index2"}}`,
				).Once().Return(
					`{"took":80,"errors":false,"items":[{"index":{"_index":"index1","_type":"_doc","_id":"ID1","_version":1,"result":"created","_shards":{"total":2,"successful":1,"failed":0},"_seq_no":0,"_primary_term":1,"status":201}},{"index":{"_index":"index1","_type":"_doc","_id":"ID2","_version":1,"result":"created","_shards":{"total":2,"successful":1,"failed":0},"_seq_no":1,"_primary_term":1,"status":201}},{"index":{"_index":"index2","_type":"_doc","_id":"ID3","_version":1,"result":"created","_shards":{"total":2,"successful":1,"failed":0},"_seq_no":0,"_primary_term":1,"status":201}}]}`,
					201,
				)

				return server
			}(),
			input: []pipeline.SinkMessage{
				DeleteMessage{
					ID:    "ID1",
					Index: "index1",
				},
				DeleteMessage{
					ID:    "ID2",
					Index: "index1",
				},
				DeleteMessage{
					ID:    "ID3",
					Index: "index2",
				},
			},
			expectedError: "",
		},

		{
			name: "Error - No ID",
			httpHandler: func() *mockServer {
				server := new(mockServer)
				return server
			}(),
			input: []pipeline.SinkMessage{
				IndexMessage{
					Index: "Index",
					Document: map[string]interface{}{
						"a": 2,
						"b": 3,
					},
				},
			},
			expectedError: "mandatory ID",
		},
		{
			name: "Error - No Index",
			httpHandler: func() *mockServer {
				server := new(mockServer)
				return server
			}(),
			input: []pipeline.SinkMessage{
				IndexMessage{
					ID: "ID1",
					Document: map[string]interface{}{
						"a": 2,
						"b": 3,
					},
				},
			},
			expectedError: "mandatory Index",
		},
		{
			name: "Error - invalid mapping",
			httpHandler: func() *mockServer {
				server := new(mockServer)
				server.On(
					"ServeHTTP",
					"/_bulk",
					`{"index":{"_id":"ID1","_index":"index1"}}
{"Value":"potato"}`,
				).Once().Return(
					`{"took":1,"errors":true,"items":[{"index":{"_index":"index1","_id":"ID1","status":400,"error":{"type":"mapper_parsing_exception","reason":"failed to parse field [Value] of type [double]","caused_by":{"type":"number_format_exception","reason":"For input string: \"potato\""}}}}]}`,
					200,
				)

				return server
			}(),
			input: []pipeline.SinkMessage{
				IndexMessage{
					ID:    "ID1",
					Index: "index1",
					Document: map[string]interface{}{
						"Value": "potato",
					},
				},
			},
			expectedError: "some items failed to be stored",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			httpServer := httptest.NewServer(test.httpHandler)
			defer httpServer.Close()
			client, err := elastic.NewSimpleClient(
				elastic.SetURL(httpServer.URL),
				elastic.SetHttpClient(httpServer.Client()),
			)
			assert.NoError(t, err)
			sink := MustNew(client)

			err = sink.Store(context.Background(), test.input...)

			if test.expectedError != "" {
				assert.EqualError(t, errors.GetRootError(err), test.expectedError)
			} else {
				assert.NoError(t, err)
			}
			test.httpHandler.AssertExpectations(t)
		})
	}
}

type mockServer struct {
	mock.Mock
}

func (s *mockServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	lines := []string{}
	// marshall and unmarshall each line so that the keys become sorted
	decoder := json.NewDecoder(r.Body)
	for {
		jsonObject := map[string]interface{}{}
		err := decoder.Decode(&jsonObject)
		if err == io.EOF {
			break
		}
		line, err := json.Marshal(jsonObject)
		if err == io.EOF {
			break
		}
		if err != nil {
			panic(err)
		}
		lines = append(lines, string(line))
	}

	request := strings.Join(lines, "\n")

	args := s.Called(r.URL.Path, request)
	result := args.String(0)
	httpCode := args.Int(1)
	w.WriteHeader(httpCode)
	w.Write([]byte(result))
}
