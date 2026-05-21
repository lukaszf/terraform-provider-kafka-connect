//go:build integration

package connectors

import (
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var (
	testFile    = "/etc/kafka-connect/kafka-connect.properties"
	hostConnect = "http://localhost:8083"
)

func requireKafkaConnect(t *testing.T) {
	t.Helper()

	client := &http.Client{
		Timeout: 5 * time.Second,
	}

	resp, err := client.Get(hostConnect)
	if err != nil {
		t.Skipf(
			"Kafka Connect is not available at %s: %v",
			hostConnect,
			err,
		)
		return
	}

	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Skipf(
			"Kafka Connect returned status %d at %s",
			resp.StatusCode,
			hostConnect,
		)
	}
}

func TestHealthz(t *testing.T) {
	requireKafkaConnect(t)

	client := &http.Client{
		Timeout: 5 * time.Second,
	}

	resp, err := client.Get(hostConnect)

	assert.NoError(t, err)
	assert.Equal(t, 200, resp.StatusCode)
}

func TestCreateConnector(t *testing.T) {
	requireKafkaConnect(t)

	client := NewClient(hostConnect, 60*time.Second)

	resp, err := client.CreateConnector(
		CreateConnectorRequest{
			ConnectorRequest: ConnectorRequest{
				Name: "test-create-connector",
			},
			Config: map[string]interface{}{
				"connector.class": "FileStreamSource",
				"file":            testFile,
				"topic":           "connect-test",
			},
		},
		true,
		60*time.Second,
	)

	assert.NoError(t, err)
	assert.Equal(t, 201, resp.Code)
}

func TestErrorCode(t *testing.T) {
	requireKafkaConnect(t)

	client := NewClient(hostConnect, 60*time.Second)

	_, err := client.CreateConnector(
		CreateConnectorRequest{
			ConnectorRequest: ConnectorRequest{
				Name: "not-a-valid-connector",
			},
			Config: map[string]interface{}{
				"connector.class": "not a valid connector class",
				"file":            testFile,
				"topic":           "connect-test",
			},
		},
		true,
		60*time.Second,
	)

	assert.Error(t, err)
}

func TestGetConnector(t *testing.T) {
	requireKafkaConnect(t)

	client := NewClient(hostConnect, 60*time.Second)

	_, err := client.CreateConnector(
		CreateConnectorRequest{
			ConnectorRequest: ConnectorRequest{
				Name: "test-get-connector",
			},
			Config: map[string]interface{}{
				"connector.class": "FileStreamSource",
				"file":            testFile,
				"topic":           "connect-test",
			},
		},
		true,
		60*time.Second,
	)

	if err != nil {
		assert.Fail(
			t,
			fmt.Sprintf(
				"error while creating test connector: %s",
				err.Error(),
			),
		)
		return
	}

	resp, err := client.GetConnector(
		ConnectorRequest{
			Name: "test-get-connector",
		},
	)

	assert.NoError(t, err)
	assert.Equal(t, 200, resp.Code)
	assert.Equal(t, "test-get-connector", resp.Name)
}

func TestGetAllConnectors(t *testing.T) {
	requireKafkaConnect(t)

	client := NewClient(hostConnect, 60*time.Second)

	_, err := client.CreateConnector(
		CreateConnectorRequest{
			ConnectorRequest: ConnectorRequest{
				Name: "test-get-all-connectors",
			},
			Config: map[string]interface{}{
				"connector.class": "FileStreamSource",
				"file":            testFile,
				"topic":           "connect-test",
			},
		},
		true,
		60*time.Second,
	)

	if err != nil {
		assert.Fail(
			t,
			fmt.Sprintf(
				"error while creating test connector: %s",
				err.Error(),
			),
		)
		return
	}

	resp, err := client.GetAll()

	assert.NoError(t, err)
	assert.Equal(t, 200, resp.Code)
	assert.Contains(t, resp.Connectors, "test-get-all-connectors")
}

func TestDeleteConnector(t *testing.T) {
	requireKafkaConnect(t)

	client := NewClient(hostConnect, 60*time.Second)

	_, err := client.CreateConnector(
		CreateConnectorRequest{
			ConnectorRequest: ConnectorRequest{
				Name: "test-delete-connectors",
			},
			Config: map[string]interface{}{
				"connector.class": "FileStreamSource",
				"file":            testFile,
				"topic":           "connect-test",
			},
		},
		true,
		60*time.Second,
	)

	if err != nil {
		assert.Fail(
			t,
			fmt.Sprintf(
				"error while creating test connector: %s",
				err.Error(),
			),
		)
		return
	}

	resp, err := client.DeleteConnector(
		ConnectorRequest{
			Name: "test-delete-connectors",
		},
		true,
		60*time.Second,
	)

	assert.NoError(t, err)
	assert.Equal(t, 204, resp.Code)

	respGet, err := client.GetConnector(
		ConnectorRequest{
			Name: "test-delete-connectors",
		},
	)

	assert.NoError(t, err)
	assert.Equal(t, 404, respGet.Code)
}

func TestGetConnectorConfig(t *testing.T) {
	requireKafkaConnect(t)

	client := NewClient(hostConnect, 60*time.Second)

	config := map[string]interface{}{
		"connector.class": "FileStreamSource",
		"tasks.max":       "1",
		"file":            testFile,
		"topic":           "connect-test",
	}

	_, err := client.CreateConnector(
		CreateConnectorRequest{
			ConnectorRequest: ConnectorRequest{
				Name: "test-get-connector-config",
			},
			Config: config,
		},
		true,
		60*time.Second,
	)

	if err != nil {
		assert.Fail(
			t,
			fmt.Sprintf(
				"error while creating test connector: %s",
				err.Error(),
			),
		)
		return
	}

	resp, err := client.GetConnectorConfig(
		ConnectorRequest{
			Name: "test-get-connector-config",
		},
	)

	assert.NoError(t, err)
	assert.Equal(t, 200, resp.Code)

	config["name"] = "test-get-connector-config"

	assert.Equal(t, config, resp.Config)
}