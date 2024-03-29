// Code generated by mockery v2.14.0. DO NOT EDIT.

package gcssink

import (
	context "context"

	mock "github.com/stretchr/testify/mock"

	storage "cloud.google.com/go/storage"
)

// MockGcsClientGateway is an autogenerated mock type for the GcsClientGateway type
type MockGcsClientGateway struct {
	mock.Mock
}

type MockGcsClientGateway_Expecter struct {
	mock *mock.Mock
}

func (_m *MockGcsClientGateway) EXPECT() *MockGcsClientGateway_Expecter {
	return &MockGcsClientGateway_Expecter{mock: &_m.Mock}
}

// Close provides a mock function with given fields:
func (_m *MockGcsClientGateway) Close() error {
	ret := _m.Called()

	var r0 error
	if rf, ok := ret.Get(0).(func() error); ok {
		r0 = rf()
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// MockGcsClientGateway_Close_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Close'
type MockGcsClientGateway_Close_Call struct {
	*mock.Call
}

// Close is a helper method to define mock.On call
func (_e *MockGcsClientGateway_Expecter) Close() *MockGcsClientGateway_Close_Call {
	return &MockGcsClientGateway_Close_Call{Call: _e.mock.On("Close")}
}

func (_c *MockGcsClientGateway_Close_Call) Run(run func()) *MockGcsClientGateway_Close_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *MockGcsClientGateway_Close_Call) Return(_a0 error) *MockGcsClientGateway_Close_Call {
	_c.Call.Return(_a0)
	return _c
}

// GetWriter provides a mock function with given fields: ctx, bucket, object, contentType, chunkSize, retrierOption
func (_m *MockGcsClientGateway) GetWriter(ctx context.Context, bucket string, object string, contentType string, chunkSize int, retrierOption ...storage.RetryOption) *storage.Writer {
	_va := make([]interface{}, len(retrierOption))
	for _i := range retrierOption {
		_va[_i] = retrierOption[_i]
	}
	var _ca []interface{}
	_ca = append(_ca, ctx, bucket, object, contentType, chunkSize)
	_ca = append(_ca, _va...)
	ret := _m.Called(_ca...)

	var r0 *storage.Writer
	if rf, ok := ret.Get(0).(func(context.Context, string, string, string, int, ...storage.RetryOption) *storage.Writer); ok {
		r0 = rf(ctx, bucket, object, contentType, chunkSize, retrierOption...)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*storage.Writer)
		}
	}

	return r0
}

// MockGcsClientGateway_GetWriter_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetWriter'
type MockGcsClientGateway_GetWriter_Call struct {
	*mock.Call
}

// GetWriter is a helper method to define mock.On call
//   - ctx context.Context
//   - bucket string
//   - object string
//   - contentType string
//   - chunkSize int
//   - retrierOption ...storage.RetryOption
func (_e *MockGcsClientGateway_Expecter) GetWriter(ctx interface{}, bucket interface{}, object interface{}, contentType interface{}, chunkSize interface{}, retrierOption ...interface{}) *MockGcsClientGateway_GetWriter_Call {
	return &MockGcsClientGateway_GetWriter_Call{Call: _e.mock.On("GetWriter",
		append([]interface{}{ctx, bucket, object, contentType, chunkSize}, retrierOption...)...)}
}

func (_c *MockGcsClientGateway_GetWriter_Call) Run(run func(ctx context.Context, bucket string, object string, contentType string, chunkSize int, retrierOption ...storage.RetryOption)) *MockGcsClientGateway_GetWriter_Call {
	_c.Call.Run(func(args mock.Arguments) {
		variadicArgs := make([]storage.RetryOption, len(args)-5)
		for i, a := range args[5:] {
			if a != nil {
				variadicArgs[i] = a.(storage.RetryOption)
			}
		}
		run(args[0].(context.Context), args[1].(string), args[2].(string), args[3].(string), args[4].(int), variadicArgs...)
	})
	return _c
}

func (_c *MockGcsClientGateway_GetWriter_Call) Return(_a0 *storage.Writer) *MockGcsClientGateway_GetWriter_Call {
	_c.Call.Return(_a0)
	return _c
}

// Write provides a mock function with given fields: writer, message
func (_m *MockGcsClientGateway) Write(writer *storage.Writer, message SinkMessage) error {
	ret := _m.Called(writer, message)

	var r0 error
	if rf, ok := ret.Get(0).(func(*storage.Writer, SinkMessage) error); ok {
		r0 = rf(writer, message)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// MockGcsClientGateway_Write_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Write'
type MockGcsClientGateway_Write_Call struct {
	*mock.Call
}

// Write is a helper method to define mock.On call
//   - writer *storage.Writer
//   - message gcssink.SinkMessage
func (_e *MockGcsClientGateway_Expecter) Write(writer interface{}, message interface{}) *MockGcsClientGateway_Write_Call {
	return &MockGcsClientGateway_Write_Call{Call: _e.mock.On("Write", writer, message)}
}

func (_c *MockGcsClientGateway_Write_Call) Run(run func(writer *storage.Writer, message SinkMessage)) *MockGcsClientGateway_Write_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(*storage.Writer), args[1].(SinkMessage))
	})
	return _c
}

func (_c *MockGcsClientGateway_Write_Call) Return(_a0 error) *MockGcsClientGateway_Write_Call {
	_c.Call.Return(_a0)
	return _c
}

type mockConstructorTestingTNewMockGcsClientGateway interface {
	mock.TestingT
	Cleanup(func())
}

// NewMockGcsClientGateway creates a new instance of MockGcsClientGateway. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
func NewMockGcsClientGateway(t mockConstructorTestingTNewMockGcsClientGateway) *MockGcsClientGateway {
	mock := &MockGcsClientGateway{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
