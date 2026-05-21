package connectors

import (
	"crypto/tls"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
	"gopkg.in/resty.v1"
)

type BaseClient interface {
	GetAll() (GetAllConnectorsResponse, error)
	GetConnector(req ConnectorRequest) (ConnectorResponse, error)
	CreateConnector(req CreateConnectorRequest) (ConnectorResponse, error)
	UpdateConnector(req CreateConnectorRequest) (ConnectorResponse, error)
	DeleteConnector(req ConnectorRequest) (EmptyResponse, error)
	GetConnectorConfig(req ConnectorRequest) (GetConnectorConfigResponse, error)
	GetConnectorStatus(req ConnectorRequest) (GetConnectorStatusResponse, error)
	RestartConnector(req ConnectorRequest) (EmptyResponse, error)
	PauseConnector(req ConnectorRequest) (EmptyResponse, error)
	ResumeConnector(req ConnectorRequest) (EmptyResponse, error)
	GetAllTasks(req ConnectorRequest) (GetAllTasksResponse, error)
	GetTaskStatus(req TaskRequest) (TaskStatusResponse, error)
	RestartTask(req TaskRequest) (EmptyResponse, error)

	SetInsecureSSL()
	SetDebug()
	SetClientCertificates(certs ...tls.Certificate)
	SetBasicAuth(username string, password string)
	SetHeader(name string, value string)
}

type baseClient struct {
	restClient *resty.Client
}

func newBaseClient(baseURL string, timeoutOptional ...time.Duration) BaseClient {
	baseURL = strings.TrimRight(strings.TrimSpace(baseURL), "/")

	timeout := 10 * time.Second

	if len(timeoutOptional) > 0 {
		timeout = timeoutOptional[0]
	}

	log.Printf("[INFO] Creating Kafka Connect REST client")
	log.Printf("[INFO] Kafka Connect URL: %s", baseURL)
	log.Printf("[INFO] Kafka Connect HTTP timeout: %s", timeout)

	restClient := resty.New().
		SetError(ErrorResponse{}).
		SetHostURL(baseURL).
		SetHeader("Accept", "application/json").
		SetRetryCount(5).
		SetRetryWaitTime(500 * time.Millisecond).
		SetRetryMaxWaitTime(5 * time.Second).
		SetTimeout(timeout).
		AddRetryCondition(func(resp *resty.Response) (bool, error) {
			if resp == nil {
				return false, nil
			}

			if resp.StatusCode() == 409 {
				log.Printf(
					"[WARN] Kafka Connect retry triggered: status=%d method=%s url=%s",
					resp.StatusCode(),
					resp.Request.Method,
					resp.Request.URL,
				)

				return true, nil
			}

			return false, nil
		})

	restClient.OnBeforeRequest(func(client *resty.Client, req *resty.Request) error {
		log.Printf(
			"[INFO] Kafka Connect request: method=%s url=%s timeout=%s",
			req.Method,
			req.URL,
			timeout,
		)

		return nil
	})

	restClient.OnAfterResponse(func(client *resty.Client, resp *resty.Response) error {
		log.Printf(
			"[INFO] Kafka Connect response: method=%s url=%s status=%d duration=%s",
			resp.Request.Method,
			resp.Request.URL,
			resp.StatusCode(),
			resp.Time(),
		)

		if resp.IsError() {
			log.Printf(
				"[ERROR] Kafka Connect error response: %s",
				string(resp.Body()),
			)
		}

		return nil
	})

	return &baseClient{
		restClient: restClient,
	}
}

func (c *baseClient) SetInsecureSSL() {
	c.restClient.SetTLSClientConfig(&tls.Config{InsecureSkipVerify: true})
}

func (c *baseClient) SetDebug() {
	c.restClient.SetDebug(true)
}

func (c *baseClient) SetClientCertificates(certs ...tls.Certificate) {
	c.restClient.SetCertificates(certs...)
}

func (c *baseClient) SetBasicAuth(username string, password string) {
	c.restClient.SetBasicAuth(username, password)
}

func (c *baseClient) SetHeader(name string, value string) {
	c.restClient.SetHeader(name, value)
}

type ErrorResponse struct {
	ErrorCode int    `json:"error_code,omitempty"`
	Message   string `json:"message,omitempty"`
}

func (err ErrorResponse) Error() string {
	return fmt.Sprintf("error code: %d, message: %s", err.ErrorCode, err.Message)
}

type ConnectorRequest struct {
	Name string `json:"name"`
}

type EmptyResponse struct {
	Code int
	ErrorResponse
}

type CreateConnectorRequest struct {
	ConnectorRequest
	Config map[string]interface{} `json:"config"`
}

type GetAllConnectorsResponse struct {
	EmptyResponse
	Connectors []string
}

type ConnectorResponse struct {
	EmptyResponse
	Name   string                 `json:"name"`
	Config map[string]interface{} `json:"config"`
	Tasks  []TaskID               `json:"tasks"`
}

type GetConnectorConfigResponse struct {
	EmptyResponse
	Config map[string]interface{}
}

type GetConnectorStatusResponse struct {
	EmptyResponse
	Name            string            `json:"name"`
	ConnectorStatus map[string]string `json:"connector"`
	TasksStatus     []TaskStatus      `json:"tasks"`
}

func (c *baseClient) GetAll() (GetAllConnectorsResponse, error) {
	result := GetAllConnectorsResponse{}
	var connectors []string

	resp, err := c.restClient.NewRequest().
		SetResult(&connectors).
		Get("/connectors")
	if err != nil {
		return result, err
	}

	if resp.StatusCode() >= 400 {
		return result, errors.Errorf("get all connectors failed: %s", resp.String())
	}

	result.Code = resp.StatusCode()
	result.Connectors = connectors

	return result, nil
}

func (c *baseClient) GetConnector(req ConnectorRequest) (ConnectorResponse, error) {
	result := ConnectorResponse{}

	resp, err := c.restClient.NewRequest().
		SetResult(&result).
		SetPathParams(map[string]string{"name": req.Name}).
		Get("/connectors/{name}")
	if err != nil {
		return result, err
	}

	if resp.StatusCode() >= 400 && resp.StatusCode() != 404 {
		return result, errors.Errorf("get connector failed: %s", resp.String())
	}

	result.Code = resp.StatusCode()

	return result, nil
}

func (c *baseClient) CreateConnector(req CreateConnectorRequest) (ConnectorResponse, error) {
	result := ConnectorResponse{}

	log.Printf("[INFO] Kafka Connect create connector request: name=%s", req.Name)

	resp, err := c.restClient.NewRequest().
		SetBody(req).
		SetResult(&result).
		Post("/connectors")
	if err != nil {
		log.Printf("[ERROR] Kafka Connect create connector failed: name=%s error=%v", req.Name, err)
		return result, err
	}

	if resp.StatusCode() >= 400 {
		log.Printf(
			"[ERROR] Kafka Connect create connector failed: name=%s status=%d body=%s",
			req.Name,
			resp.StatusCode(),
			resp.String(),
		)

		return result, errors.Errorf("create connector failed: %s", resp.String())
	}

	result.Code = resp.StatusCode()

	log.Printf("[INFO] Kafka Connect create connector completed: name=%s status=%d", req.Name, result.Code)

	return result, nil
}

func (c *baseClient) UpdateConnector(req CreateConnectorRequest) (ConnectorResponse, error) {
	result := ConnectorResponse{}

	log.Printf("[INFO] Kafka Connect update connector request: name=%s", req.Name)

	resp, err := c.restClient.NewRequest().
		SetPathParams(map[string]string{"name": req.Name}).
		SetBody(req.Config).
		SetResult(&result).
		Put("/connectors/{name}/config")
	if err != nil {
		log.Printf("[ERROR] Kafka Connect update connector failed: name=%s error=%v", req.Name, err)
		return result, err
	}

	if resp.StatusCode() >= 400 {
		log.Printf(
			"[ERROR] Kafka Connect update connector failed: name=%s status=%d body=%s",
			req.Name,
			resp.StatusCode(),
			resp.String(),
		)

		return result, errors.Errorf("update connector failed: %s", resp.String())
	}

	result.Code = resp.StatusCode()

	log.Printf("[INFO] Kafka Connect update connector completed: name=%s status=%d", req.Name, result.Code)

	return result, nil
}

func (c *baseClient) DeleteConnector(req ConnectorRequest) (EmptyResponse, error) {
	result := EmptyResponse{}

	log.Printf("[INFO] Kafka Connect delete connector request: name=%s", req.Name)

	resp, err := c.restClient.NewRequest().
		SetResult(&result).
		SetPathParams(map[string]string{"name": req.Name}).
		Delete("/connectors/{name}")
	if err != nil {
		log.Printf("[ERROR] Kafka Connect delete connector failed: name=%s error=%v", req.Name, err)
		return result, err
	}

	if resp.StatusCode() >= 400 && resp.StatusCode() != 404 {
		log.Printf(
			"[ERROR] Kafka Connect delete connector failed: name=%s status=%d body=%s",
			req.Name,
			resp.StatusCode(),
			resp.String(),
		)

		return result, errors.Errorf("delete connector failed: %s", resp.String())
	}

	result.Code = resp.StatusCode()

	log.Printf("[INFO] Kafka Connect delete connector completed: name=%s status=%d", req.Name, result.Code)

	return result, nil
}

func (c *baseClient) GetConnectorConfig(req ConnectorRequest) (GetConnectorConfigResponse, error) {
	result := GetConnectorConfigResponse{}
	var config map[string]interface{}

	resp, err := c.restClient.NewRequest().
		SetResult(&config).
		SetPathParams(map[string]string{"name": req.Name}).
		Get("/connectors/{name}/config")
	if err != nil {
		return result, err
	}

	if resp.StatusCode() >= 400 && resp.StatusCode() != 404 {
		return result, errors.Errorf("get connector config failed: %s", resp.String())
	}

	result.Code = resp.StatusCode()
	result.Config = config

	return result, nil
}

func (c *baseClient) GetConnectorStatus(req ConnectorRequest) (GetConnectorStatusResponse, error) {
	result := GetConnectorStatusResponse{}

	resp, err := c.restClient.NewRequest().
		SetResult(&result).
		SetPathParams(map[string]string{"name": req.Name}).
		Get("/connectors/{name}/status")
	if err != nil {
		return result, err
	}

	if resp.StatusCode() >= 400 && resp.StatusCode() != 404 {
		return result, errors.Errorf("get connector status failed: %s", resp.String())
	}

	result.Code = resp.StatusCode()

	return result, nil
}

func (c *baseClient) RestartConnector(req ConnectorRequest) (EmptyResponse, error) {
	result := EmptyResponse{}

	resp, err := c.restClient.NewRequest().
		SetResult(&result).
		SetPathParams(map[string]string{"name": req.Name}).
		Post("/connectors/{name}/restart")
	if err != nil {
		return result, err
	}

	if resp.StatusCode() >= 400 {
		return result, errors.Errorf("restart connector failed: %s", resp.String())
	}

	result.Code = resp.StatusCode()

	return result, nil
}

func (c *baseClient) PauseConnector(req ConnectorRequest) (EmptyResponse, error) {
	result := EmptyResponse{}

	resp, err := c.restClient.NewRequest().
		SetResult(&result).
		SetPathParams(map[string]string{"name": req.Name}).
		Put("/connectors/{name}/pause")
	if err != nil {
		return result, err
	}

	if resp.StatusCode() >= 400 {
		return result, errors.Errorf("pause connector failed: %s", resp.String())
	}

	result.Code = resp.StatusCode()

	return result, nil
}

func (c *baseClient) ResumeConnector(req ConnectorRequest) (EmptyResponse, error) {
	result := EmptyResponse{}

	resp, err := c.restClient.NewRequest().
		SetResult(&result).
		SetPathParams(map[string]string{"name": req.Name}).
		Put("/connectors/{name}/resume")
	if err != nil {
		return result, err
	}

	if resp.StatusCode() >= 400 {
		return result, errors.Errorf("resume connector failed: %s", resp.String())
	}

	result.Code = resp.StatusCode()

	return result, nil
}

type TaskRequest struct {
	Connector string
	TaskID    int
}

type GetAllTasksResponse struct {
	Code  int
	Tasks []TaskDetails
}

type TaskDetails struct {
	ID     TaskID                 `json:"id"`
	Config map[string]interface{} `json:"config"`
}

type TaskID struct {
	Connector string `json:"connector"`
	TaskID    int    `json:"task"`
}

type TaskStatusResponse struct {
	Code   int
	Status TaskStatus
}

type TaskStatus struct {
	ID       int    `json:"id"`
	State    string `json:"state"`
	WorkerID string `json:"worker_id"`
	Trace    string `json:"trace,omitempty"`
}

func (c *baseClient) GetAllTasks(req ConnectorRequest) (GetAllTasksResponse, error) {
	var result GetAllTasksResponse

	resp, err := c.restClient.NewRequest().
		SetResult(&result.Tasks).
		SetPathParams(map[string]string{"name": req.Name}).
		Get("/connectors/{name}/tasks")
	if err != nil {
		return result, err
	}

	if resp.StatusCode() >= 400 {
		return result, errors.Errorf("get all tasks failed: %s", resp.String())
	}

	result.Code = resp.StatusCode()

	return result, nil
}

func (c *baseClient) GetTaskStatus(req TaskRequest) (TaskStatusResponse, error) {
	var result TaskStatusResponse

	resp, err := c.restClient.NewRequest().
		SetResult(&result).
		SetPathParams(map[string]string{
			"name":    req.Connector,
			"task_id": strconv.Itoa(req.TaskID),
		}).
		Get("/connectors/{name}/tasks/{task_id}/status")
	if err != nil {
		return result, err
	}

	if resp.StatusCode() >= 400 && resp.StatusCode() != 404 {
		return result, errors.Errorf("get task status failed: %s", resp.String())
	}

	result.Code = resp.StatusCode()

	return result, nil
}

func (c *baseClient) RestartTask(req TaskRequest) (EmptyResponse, error) {
	var result EmptyResponse

	resp, err := c.restClient.NewRequest().
		SetResult(&result).
		SetPathParams(map[string]string{
			"name":    req.Connector,
			"task_id": strconv.Itoa(req.TaskID),
		}).
		Post("/connectors/{name}/tasks/{task_id}/restart")
	if err != nil {
		return result, err
	}

	if resp.StatusCode() >= 400 {
		return result, errors.Errorf("restart task failed: %s", resp.String())
	}

	result.Code = resp.StatusCode()

	return result, nil
}
