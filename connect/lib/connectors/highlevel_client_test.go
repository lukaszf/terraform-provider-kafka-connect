//go:build !integration

package connectors

import (
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func Test_IsUpToDate_Should_Be_True(t *testing.T) {
	configOnline := map[string]interface{}{
		"name":   "test1",
		"param1": 2,
		"param2": "abc",
		"param3": "3",
	}

	configLocal := map[string]interface{}{
		"param1": 2,
		"param2": "abc",
		"param3": 3,
	}

	mockBaseClient := &MockBaseClient{}

	mockBaseClient.On("GetConnectorConfig", mock.Anything).
		Return(GetConnectorConfigResponse{
			EmptyResponse: EmptyResponse{Code: 200},
			Config:        configOnline,
		}, nil)

	client := &highLevelClient{
		client: mockBaseClient,
	}

	isUpToDate, err := client.IsUpToDate("test1", configLocal)

	assert.NoError(t, err)
	assert.True(t, isUpToDate)
}

func Test_IsUpToDate_Should_Be_False(t *testing.T) {
	configOnline := map[string]interface{}{
		"name":   "test1",
		"param1": 3,
		"param2": "abc",
		"param3": "3",
	}

	configLocal := map[string]interface{}{
		"param1": 2,
		"param2": "abc",
		"param3": 3,
	}

	mockBaseClient := &MockBaseClient{}

	mockBaseClient.On("GetConnectorConfig", mock.Anything).
		Return(GetConnectorConfigResponse{
			EmptyResponse: EmptyResponse{Code: 200},
			Config:        configOnline,
		}, nil)

	client := &highLevelClient{
		client: mockBaseClient,
	}

	isUpToDate, err := client.IsUpToDate("test1", configLocal)

	assert.NoError(t, err)
	assert.False(t, isUpToDate)
}

func Test_tryUntil_When_Success(t *testing.T) {
	result := tryUntil(
		func() bool {
			return true
		},
		100*time.Millisecond,
	)

	assert.True(t, result)
}

func Test_tryUntil_When_Timeout(t *testing.T) {
	result := tryUntil(
		func() bool {
			time.Sleep(200 * time.Millisecond)
			return true
		},
		100*time.Millisecond,
	)

	assert.False(t, result)
}

func Test_DeployConnector_When_Already_Up_To_Date(t *testing.T) {
	configOnline := map[string]interface{}{
		"name":   "test1",
		"param1": 2,
		"param2": "abc",
		"param3": "3",
	}

	configLocal := map[string]interface{}{
		"param1": 2,
		"param2": "abc",
		"param3": 3,
	}

	mockBaseClient := &MockBaseClient{}

	mockBaseClient.On("GetConnector", mock.Anything).
		Return(ConnectorResponse{
			EmptyResponse: EmptyResponse{Code: 200},
			Name:          "test1",
			Config:        configOnline,
		}, nil)

	mockBaseClient.On("GetConnectorConfig", mock.Anything).
		Return(GetConnectorConfigResponse{
			EmptyResponse: EmptyResponse{Code: 200},
			Config:        configOnline,
		}, nil)

	client := &highLevelClient{
		client: mockBaseClient,
	}

	err := client.DeployConnector(
		CreateConnectorRequest{
			ConnectorRequest: ConnectorRequest{
				Name: "test1",
			},
			Config: configLocal,
		},
	)

	assert.NoError(t, err)

	mockBaseClient.AssertExpectations(t)
	mockBaseClient.AssertNotCalled(t, "UpdateConnector", mock.Anything)
}

func Test_DeployConnector_Ok(t *testing.T) {
	configOnline := map[string]interface{}{
		"name":   "test1",
		"param1": 2,
	}

	configLocal := map[string]interface{}{
		"param1": 3,
	}

	updatedConfig := map[string]interface{}{
		"name":   "test1",
		"param1": 3,
	}

	mockBaseClient := &MockBaseClient{}

	mockBaseClient.On("GetConnector", mock.Anything).
		Return(ConnectorResponse{
			EmptyResponse: EmptyResponse{Code: 200},
			Name:          "test1",
			Config:        configOnline,
		}, nil)

	mockBaseClient.On("GetConnectorConfig", mock.Anything).
		Return(GetConnectorConfigResponse{
			EmptyResponse: EmptyResponse{Code: 200},
			Config:        configOnline,
		}, nil).Once()

	mockBaseClient.On("UpdateConnector", mock.Anything).
		Return(ConnectorResponse{
			EmptyResponse: EmptyResponse{Code: 200},
			Name:          "test1",
			Config:        updatedConfig,
		}, nil)

	mockBaseClient.On("GetConnectorConfig", mock.Anything).
		Return(GetConnectorConfigResponse{
			EmptyResponse: EmptyResponse{Code: 200},
			Config:        updatedConfig,
		}, nil).Once()

	client := &highLevelClient{
		client: mockBaseClient,
	}

	err := client.DeployConnector(
		CreateConnectorRequest{
			ConnectorRequest: ConnectorRequest{
				Name: "test1",
			},
			Config: configLocal,
		},
		1*time.Second,
	)

	assert.NoError(t, err)

	mockBaseClient.AssertExpectations(t)
}

func Test_DeployMultipleConnector_Ok(t *testing.T) {
	mockBaseClient := &MockBaseClient{}

	mockBaseClient.On("GetConnector", mock.Anything).
		Return(ConnectorResponse{
			EmptyResponse: EmptyResponse{Code: 404},
		}, nil)

	mockBaseClient.On("UpdateConnector", mock.Anything).
		Return(ConnectorResponse{
			EmptyResponse: EmptyResponse{Code: 200},
		}, nil)

	mockBaseClient.On("GetConnectorConfig", mock.Anything).
		Return(func(req ConnectorRequest) GetConnectorConfigResponse {
			return GetConnectorConfigResponse{
				EmptyResponse: EmptyResponse{Code: 200},
				Config: map[string]interface{}{
					"name": req.Name,
				},
			}
		}, nil)

	client := &highLevelClient{
		client:             mockBaseClient,
		maxParallelRequest: 2,
	}

	err := client.DeployMultipleConnector(
		[]CreateConnectorRequest{
			{
				ConnectorRequest: ConnectorRequest{
					Name: "test1",
				},
				Config: map[string]interface{}{
					"name": "test1",
				},
			},
			{
				ConnectorRequest: ConnectorRequest{
					Name: "test2",
				},
				Config: map[string]interface{}{
					"name": "test2",
				},
			},
			{
				ConnectorRequest: ConnectorRequest{
					Name: "test3",
				},
				Config: map[string]interface{}{
					"name": "test3",
				},
			},
			{
				ConnectorRequest: ConnectorRequest{
					Name: "test4",
				},
				Config: map[string]interface{}{
					"name": "test4",
				},
			},
			{
				ConnectorRequest: ConnectorRequest{
					Name: "test5",
				},
				Config: map[string]interface{}{
					"name": "test5",
				},
			},
		},
		1*time.Second,
	)

	assert.NoError(t, err)

	mockBaseClient.AssertNumberOfCalls(
		t,
		"UpdateConnector",
		5,
	)
}

func Test_DeployMultipleConnector_Error(t *testing.T) {
	mockBaseClient := &MockBaseClient{}

	mockBaseClient.On("GetConnector", mock.Anything).
		Return(ConnectorResponse{
			EmptyResponse: EmptyResponse{Code: 404},
		}, nil)

	mockBaseClient.On("UpdateConnector", mock.Anything).
		Return(ConnectorResponse{}, errors.New("random error"))

	client := &highLevelClient{
		client:             mockBaseClient,
		maxParallelRequest: 2,
	}

	err := client.DeployMultipleConnector(
		[]CreateConnectorRequest{
			{
				ConnectorRequest: ConnectorRequest{
					Name: "test1",
				},
				Config: map[string]interface{}{
					"name": "test1",
				},
			},
			{
				ConnectorRequest: ConnectorRequest{
					Name: "test2",
				},
				Config: map[string]interface{}{
					"name": "test2",
				},
			},
		},
		1*time.Second,
	)

	assert.Error(t, err)
}
