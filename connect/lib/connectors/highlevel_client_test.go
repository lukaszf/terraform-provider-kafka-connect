package connectors

import (
	"crypto/tls"
	"fmt"
	"sync"
	"time"

	"github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"
)

// HighLevelClient support all Kafka Connect API functions + helper features.
type HighLevelClient interface {
	// Kafka Connect API
	GetAll() (GetAllConnectorsResponse, error)
	GetConnector(req ConnectorRequest) (ConnectorResponse, error)

	CreateConnector(
		req CreateConnectorRequest,
		sync bool,
		timeoutOptional ...time.Duration,
	) (ConnectorResponse, error)

	UpdateConnector(
		req CreateConnectorRequest,
		sync bool,
		timeoutOptional ...time.Duration,
	) (ConnectorResponse, error)

	DeleteConnector(
		req ConnectorRequest,
		sync bool,
		timeoutOptional ...time.Duration,
	) (EmptyResponse, error)

	GetConnectorConfig(req ConnectorRequest) (GetConnectorConfigResponse, error)
	GetConnectorStatus(req ConnectorRequest) (GetConnectorStatusResponse, error)
	RestartConnector(req ConnectorRequest) (EmptyResponse, error)

	PauseConnector(
		req ConnectorRequest,
		sync bool,
		timeoutOptional ...time.Duration,
	) (EmptyResponse, error)

	ResumeConnector(
		req ConnectorRequest,
		sync bool,
		timeoutOptional ...time.Duration,
	) (EmptyResponse, error)

	GetAllTasks(req ConnectorRequest) (GetAllTasksResponse, error)
	GetTaskStatus(req TaskRequest) (TaskStatusResponse, error)
	RestartTask(req TaskRequest) (EmptyResponse, error)

	// helper methods
	IsUpToDate(
		connector string,
		config map[string]interface{},
	) (bool, error)

	DeployConnector(
		req CreateConnectorRequest,
		timeoutOptional ...time.Duration,
	) error

	DeployMultipleConnector(
		connectors []CreateConnectorRequest,
		timeoutOptional ...time.Duration,
	) error

	SetInsecureSSL()
	SetDebug()
	SetClientCertificates(certs ...tls.Certificate)
	SetParallelism(value int)
	SetBasicAuth(username string, password string)
	SetHeader(name string, value string)
}

type highLevelClient struct {
	client             BaseClient
	maxParallelRequest int
}

// NewClient creates a new HighLevelClient.
func NewClient(
	url string,
	timeoutOptional ...time.Duration,
) HighLevelClient {

	timeout := 10 * time.Second

	if len(timeoutOptional) > 0 {
		timeout = timeoutOptional[0]
	}

	return &highLevelClient{
		client:             newBaseClient(url, timeout),
		maxParallelRequest: 3,
	}
}

// -----------------------------------------------------------------------------
// Config
// -----------------------------------------------------------------------------

func (c *highLevelClient) SetParallelism(value int) {
	c.maxParallelRequest = value
}

func (c *highLevelClient) SetInsecureSSL() {
	c.client.SetInsecureSSL()
}

func (c *highLevelClient) SetDebug() {
	c.client.SetDebug()
}

func (c *highLevelClient) SetClientCertificates(certs ...tls.Certificate) {
	c.client.SetClientCertificates(certs...)
}

func (c *highLevelClient) SetBasicAuth(username string, password string) {
	c.client.SetBasicAuth(username, password)
}

func (c *highLevelClient) SetHeader(name string, value string) {
	c.client.SetHeader(name, value)
}

// -----------------------------------------------------------------------------
// Connectors
// -----------------------------------------------------------------------------

func (c *highLevelClient) GetAll() (GetAllConnectorsResponse, error) {
	return c.client.GetAll()
}

func (c *highLevelClient) GetConnector(
	req ConnectorRequest,
) (ConnectorResponse, error) {
	return c.client.GetConnector(req)
}

func (c *highLevelClient) CreateConnector(
	req CreateConnectorRequest,
	sync bool,
	timeoutOptional ...time.Duration,
) (ConnectorResponse, error) {

	result, err := c.client.CreateConnector(req)
	if err != nil {
		return result, err
	}

	if sync {
		timeout := resolveTimeout(timeoutOptional...)

		if !tryUntil(
			func() bool {
				resp, err := c.GetConnector(req.ConnectorRequest)
				return err == nil && resp.Code == 200
			},
			timeout,
		) {
			return result, errors.New(
				"timeout waiting for connector creation",
			)
		}
	}

	return result, nil
}

func (c *highLevelClient) UpdateConnector(
	req CreateConnectorRequest,
	sync bool,
	timeoutOptional ...time.Duration,
) (ConnectorResponse, error) {

	result, err := c.client.UpdateConnector(req)
	if err != nil {
		return result, err
	}

	if sync {
		timeout := resolveTimeout(timeoutOptional...)

		if !tryUntil(
			func() bool {
				upToDate, err := c.IsUpToDate(req.Name, req.Config)
				return err == nil && upToDate
			},
			timeout,
		) {
			return result, errors.New(
				"timeout waiting for connector update",
			)
		}
	}

	return result, nil
}

func (c *highLevelClient) DeleteConnector(
	req ConnectorRequest,
	sync bool,
	timeoutOptional ...time.Duration,
) (EmptyResponse, error) {

	result, err := c.client.DeleteConnector(req)
	if err != nil {
		return result, err
	}

	if sync {
		timeout := resolveTimeout(timeoutOptional...)

		if !tryUntil(
			func() bool {
				resp, err := c.GetConnector(req)
				return err == nil && resp.Code == 404
			},
			timeout,
		) {
			return result, errors.New(
				"timeout waiting for connector deletion",
			)
		}
	}

	return result, nil
}

func (c *highLevelClient) GetConnectorConfig(
	req ConnectorRequest,
) (GetConnectorConfigResponse, error) {
	return c.client.GetConnectorConfig(req)
}

func (c *highLevelClient) GetConnectorStatus(
	req ConnectorRequest,
) (GetConnectorStatusResponse, error) {
	return c.client.GetConnectorStatus(req)
}

func (c *highLevelClient) RestartConnector(
	req ConnectorRequest,
) (EmptyResponse, error) {
	return c.client.RestartConnector(req)
}

func (c *highLevelClient) PauseConnector(
	req ConnectorRequest,
	sync bool,
	timeoutOptional ...time.Duration,
) (EmptyResponse, error) {

	result, err := c.client.PauseConnector(req)
	if err != nil {
		return result, err
	}

	if sync {
		timeout := resolveTimeout(timeoutOptional...)

		if !tryUntil(
			func() bool {
				resp, err := c.GetConnectorStatus(req)

				return err == nil &&
					resp.Code == 200 &&
					resp.ConnectorStatus["state"] == "PAUSED"
			},
			timeout,
		) {
			return result, errors.New(
				"timeout waiting for connector pause",
			)
		}
	}

	return result, nil
}

func (c *highLevelClient) ResumeConnector(
	req ConnectorRequest,
	sync bool,
	timeoutOptional ...time.Duration,
) (EmptyResponse, error) {

	result, err := c.client.ResumeConnector(req)
	if err != nil {
		return result, err
	}

	if sync {
		timeout := resolveTimeout(timeoutOptional...)

		if !tryUntil(
			func() bool {
				resp, err := c.GetConnectorStatus(req)

				return err == nil &&
					resp.Code == 200 &&
					resp.ConnectorStatus["state"] == "RUNNING"
			},
			timeout,
		) {
			return result, errors.New(
				"timeout waiting for connector resume",
			)
		}
	}

	return result, nil
}

// -----------------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------------

func resolveTimeout(timeoutOptional ...time.Duration) time.Duration {
	timeout := 2 * time.Minute

	if len(timeoutOptional) > 0 {
		timeout = timeoutOptional[0]
	}

	return timeout
}

func (c *highLevelClient) IsUpToDate(
	connector string,
	config map[string]interface{},
) (bool, error) {

	copyConfig := make(map[string]interface{}, len(config))

	for key, value := range config {
		copyConfig[key] = value
	}

	copyConfig["name"] = connector

	configResp, err := c.GetConnectorConfig(
		ConnectorRequest{Name: connector},
	)
	if err != nil {
		return false, err
	}

	if configResp.Code == 404 {
		return false, nil
	}

	if configResp.Code >= 400 {
		return false, errors.New(
			fmt.Sprintf("status code: %d", configResp.Code),
		)
	}

	if len(configResp.Config) != len(copyConfig) {
		return false, nil
	}

	for key, value := range configResp.Config {
		if convertConfigValueToString(copyConfig[key]) !=
			convertConfigValueToString(value) {
			return false, nil
		}
	}

	return true, nil
}

func convertConfigValueToString(value interface{}) string {
	return fmt.Sprintf("%v", value)
}

func tryUntil(exec func() bool, limit time.Duration) bool {
	timeout := time.NewTimer(limit)
	defer timeout.Stop()

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		if exec() {
			return true
		}

		select {
		case <-timeout.C:
			return false

		case <-ticker.C:
			continue
		}
	}
}

func (c *highLevelClient) DeployConnector(
	req CreateConnectorRequest,
	timeoutOptional ...time.Duration,
) error {

	existingConnector, err := c.GetConnector(
		ConnectorRequest{Name: req.Name},
	)
	if err != nil {
		return err
	}

	if existingConnector.Code != 404 {
		upToDate, err := c.IsUpToDate(req.Name, req.Config)
		if err != nil {
			return err
		}

		if upToDate {
			return nil
		}
	}

	_, err = c.UpdateConnector(req, true, timeoutOptional...)

	return err
}

func (c *highLevelClient) DeployMultipleConnector(
	connectors []CreateConnectorRequest,
	timeoutOptional ...time.Duration,
) (err error) {

	errSync := new(sync.Mutex)

	throttleCh := make(chan interface{}, c.maxParallelRequest)

	for _, connector := range connectors {
		throttleCh <- struct{}{}

		go func(req CreateConnectorRequest) {
			defer func() {
				<-throttleCh
			}()

			newErr := c.DeployConnector(
				req,
				timeoutOptional...,
			)

			if newErr != nil {
				errSync.Lock()
				defer errSync.Unlock()

				err = multierror.Append(
					err,
					errors.Wrapf(
						newErr,
						"error while deploying: %v",
						req.Name,
					),
				)
			}
		}(connector)
	}

	for i := 0; i < c.maxParallelRequest; i++ {
		throttleCh <- struct{}{}
	}

	return err
}

// -----------------------------------------------------------------------------
// Tasks
// -----------------------------------------------------------------------------

func (c *highLevelClient) GetAllTasks(
	req ConnectorRequest,
) (GetAllTasksResponse, error) {
	return c.client.GetAllTasks(req)
}

func (c *highLevelClient) GetTaskStatus(
	req TaskRequest,
) (TaskStatusResponse, error) {
	return c.client.GetTaskStatus(req)
}

func (c *highLevelClient) RestartTask(
	req TaskRequest,
) (EmptyResponse, error) {
	return c.client.RestartTask(req)
}