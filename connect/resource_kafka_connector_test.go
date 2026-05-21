package connect

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	r "github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
	"github.com/hashicorp/terraform-plugin-sdk/v2/terraform"

	kc "github.com/lukaszf/terraform-provider-kafka-connect/connect/lib/connectors"
)

func TestAccConnectorConfigUpdate(t *testing.T) {
	r.Test(t, r.TestCase{
		PreCheck:  func() { testAccPreCheck(t) },
		Providers: testProviders,
		Steps: []r.TestStep{
			{
				Config: testResourceConnectorInitialConfig,
				Check:  testResourceConnectorInitialCheck,
			},
			{
				Config:            testResourceConnectorInitialConfig,
				ResourceName:      "kafka-connect_connector.test",
				ImportStateVerify: true,
				ImportState:       true,
			},
			{
				Config: testResourceConnectorUpdateConfig,
				Check:  testResourceConnectorUpdateCheck,
			},
		},
	})
}

func TestAccConnectorWithTimeouts(t *testing.T) {
	r.Test(t, r.TestCase{
		PreCheck:  func() { testAccPreCheck(t) },
		Providers: testProviders,
		Steps: []r.TestStep{
			{
				Config: testResourceConnectorWithTimeouts,
				Check: r.ComposeTestCheckFunc(
					r.TestCheckResourceAttr(
						"kafka-connect_connector.test_timeouts",
						"name",
						"test-with-timeouts",
					),
					r.TestCheckResourceAttr(
						"kafka-connect_connector.test_timeouts",
						"config.tasks.max",
						"1",
					),
					r.TestCheckResourceAttrSet(
						"kafka-connect_connector.test_timeouts",
						"id",
					),
				),
			},
		},
	})
}

func testResourceConnectorInitialCheck(s *terraform.State) error {
	resourceState := s.Modules[0].Resources["kafka-connect_connector.test"]
	if resourceState == nil {
		return fmt.Errorf("resource not found in state")
	}

	instanceState := resourceState.Primary
	if instanceState == nil {
		return fmt.Errorf("resource has no primary instance")
	}

	name := instanceState.ID

	if name != instanceState.Attributes["name"] {
		return fmt.Errorf("id does not match name")
	}

	client := testProvider.Meta().(kc.HighLevelClient)

	connector, err := client.GetConnector(
		kc.ConnectorRequest{
			Name: "sqlite-sink",
		},
	)
	if err != nil {
		return err
	}

	tasksMax := connector.Config["tasks.max"]
	expected := "2"

	if tasksMax != expected {
		return fmt.Errorf(
			"tasks.max should be %s, got %v. Connector config: %v",
			expected,
			tasksMax,
			connector.Config,
		)
	}

	return nil
}

func testResourceConnectorUpdateCheck(s *terraform.State) error {
	client := testProvider.Meta().(kc.HighLevelClient)

	connector, err := client.GetConnector(
		kc.ConnectorRequest{
			Name: "sqlite-sink",
		},
	)
	if err != nil {
		return err
	}

	tasksMax := connector.Config["tasks.max"]
	expected := "1"

	if tasksMax != expected {
		return fmt.Errorf(
			"tasks.max should be %s, got %v. Connector config: %v",
			expected,
			tasksMax,
			connector.Config,
		)
	}

	return nil
}

const testResourceConnectorInitialConfig = `
resource "kafka-connect_connector" "test" {
  name = "sqlite-sink"

  config = {
    "name"            = "sqlite-sink"
    "connector.class" = "io.confluent.connect.jdbc.JdbcSinkConnector"
    "tasks.max"       = "2"
    "topics"          = "orders"
    "connection.url"  = "jdbc:sqlite:test.db"
    "auto.create"     = "true"
  }
}
`

const testResourceConnectorUpdateConfig = `
resource "kafka-connect_connector" "test" {
  name = "sqlite-sink"

  config = {
    "name"            = "sqlite-sink"
    "connector.class" = "io.confluent.connect.jdbc.JdbcSinkConnector"
    "tasks.max"       = "1"
    "topics"          = "orders"
    "connection.url"  = "jdbc:sqlite:test.db"
    "auto.create"     = "true"
  }
}
`

const testResourceConnectorWithTimeouts = `
resource "kafka-connect_connector" "test_timeouts" {
  name = "test-with-timeouts"

  config = {
    "name"            = "test-with-timeouts"
    "connector.class" = "io.confluent.connect.jdbc.JdbcSinkConnector"
    "tasks.max"       = "1"
    "topics"          = "test-topic"
    "connection.url"  = "jdbc:sqlite:test.db"
    "auto.create"     = "true"
  }

  timeouts {
    create = "10m"
    update = "8m"
    delete = "3m"
  }
}
`

func TestIsRebalanceError(t *testing.T) {
	testCases := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "rebalance in progress",
			err:      errors.New("rebalance in progress"),
			expected: true,
		},
		{
			name:     "rebalance expected",
			err:      errors.New("RebalanceExpectedException"),
			expected: true,
		},
		{
			name:     "rebalance is expected",
			err:      errors.New("rebalance is expected"),
			expected: true,
		},
		{
			name:     "conflicting operation",
			err:      errors.New("conflicting operation"),
			expected: true,
		},
		{
			name:     "http 409",
			err:      errors.New("409 conflict"),
			expected: true,
		},
		{
			name:     "normal timeout",
			err:      errors.New("connection timeout"),
			expected: false,
		},
		{
			name:     "nil error",
			err:      nil,
			expected: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actual := isRebalanceError(tc.err)

			if actual != tc.expected {
				t.Errorf(
					"expected %t, got %t for error: %v",
					tc.expected,
					actual,
					tc.err,
				)
			}
		})
	}
}

func TestWithRebalanceRetry(t *testing.T) {
	t.Run("successful operation after rebalance errors", func(t *testing.T) {
		callCount := 0

		operation := func() error {
			callCount++

			if callCount < 3 {
				return errors.New("rebalance in progress")
			}

			return nil
		}

		err := withRebalanceRetry(operation, 5*time.Second)
		if err != nil {
			t.Errorf("expected no error, got: %v", err)
		}

		if callCount != 3 {
			t.Errorf("expected 3 calls, got %d", callCount)
		}
	})

	t.Run("non-rebalance error should not retry", func(t *testing.T) {
		callCount := 0
		expectedErr := errors.New("connection timeout")

		operation := func() error {
			callCount++
			return expectedErr
		}

		err := withRebalanceRetry(operation, 5*time.Second)

		if !errors.Is(err, expectedErr) {
			t.Errorf("expected error %v, got: %v", expectedErr, err)
		}

		if callCount != 1 {
			t.Errorf("expected 1 call, got %d", callCount)
		}
	})

	t.Run("timeout should be respected", func(t *testing.T) {
		callCount := 0

		operation := func() error {
			callCount++
			return errors.New("rebalance in progress")
		}

		start := time.Now()

		err := withRebalanceRetry(operation, 1*time.Second)

		duration := time.Since(start)

		if err == nil {
			t.Errorf("expected timeout error, got nil")
		}

		if !strings.Contains(err.Error(), "timed out waiting for Kafka Connect rebalance to finish") {
			t.Errorf("expected timeout error message, got: %v", err)
		}

		if duration < 800*time.Millisecond || duration > 3*time.Second {
			t.Errorf("expected timeout around 1s, got %v", duration)
		}

		if callCount < 2 {
			t.Errorf("expected at least 2 retry attempts, got %d", callCount)
		}
	})

	t.Run("long timeout allows many retries", func(t *testing.T) {
		callCount := 0

		operation := func() error {
			callCount++

			if callCount < 5 {
				return errors.New("rebalance in progress")
			}

			return nil
		}

		err := withRebalanceRetry(operation, 30*time.Second)
		if err != nil {
			t.Errorf("expected no error with long timeout, got: %v", err)
		}

		if callCount != 5 {
			t.Errorf("expected 5 calls, got %d", callCount)
		}
	})

	t.Run("very short timeout fails quickly", func(t *testing.T) {
		callCount := 0

		operation := func() error {
			callCount++
			time.Sleep(100 * time.Millisecond)
			return errors.New("rebalance in progress")
		}

		start := time.Now()

		err := withRebalanceRetry(operation, 50*time.Millisecond)

		duration := time.Since(start)

		if err == nil {
			t.Errorf("expected timeout error, got nil")
		}

		if duration > 300*time.Millisecond {
			t.Errorf("expected fast timeout, got %v", duration)
		}

		if callCount != 1 {
			t.Errorf("expected 1 call with very short timeout, got %d", callCount)
		}
	})
}
