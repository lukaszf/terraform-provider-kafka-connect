package connect

import (
	"errors"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"time"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"

	kc "github.com/lukaszf/terraform-provider-kafka-connect/connect/lib/connectors"
)

func kafkaConnectorResource() *schema.Resource {
	return &schema.Resource{
		Create: connectorCreate,
		Read:   connectorRead,
		Update: connectorUpdate,
		Delete: connectorDelete,

		Importer: &schema.ResourceImporter{
			State: setNameFromID,
		},

		Timeouts: &schema.ResourceTimeout{
			Create: schema.DefaultTimeout(60 * time.Second),
			Update: schema.DefaultTimeout(60 * time.Second),
			Delete: schema.DefaultTimeout(60 * time.Second),
		},

		Schema: map[string]*schema.Schema{
			"name": {
				Type:        schema.TypeString,
				Required:    true,
				ForceNew:    true,
				Description: "The name of the connector.",
			},

			"config": {
				Type:        schema.TypeMap,
				Optional:    true,
				ForceNew:    false,
				Description: "A map of connector configuration properties.",
			},

			"config_sensitive": {
				Type:        schema.TypeMap,
				Optional:    true,
				ForceNew:    false,
				Sensitive:   true,
				Description: "A map of sensitive connector configuration properties, such as passwords.",
			},
		},
	}
}

func setNameFromID(d *schema.ResourceData, meta interface{}) ([]*schema.ResourceData, error) {
	connectorName := d.Id()

	log.Printf("[INFO] Import connector with name: %s", connectorName)

	if err := d.Set("name", connectorName); err != nil {
		return nil, err
	}

	return []*schema.ResourceData{d}, nil
}

func connectorCreate(d *schema.ResourceData, meta interface{}) error {
	c := meta.(kc.HighLevelClient)

	name := nameFromRD(d)

	config, sensitiveCache := configFromRD(d)

	if n, ok := config["name"]; ok && n != name {
		return errors.New("config.name must be identical to the resource name")
	} else if !ok {
		return errors.New("config.name is required and must be identical to the resource name")
	}

	req := kc.CreateConnectorRequest{
		ConnectorRequest: kc.ConnectorRequest{
			Name: name,
		},
		Config: config,
	}

	var connectorResponse kc.ConnectorResponse

	err := withRebalanceRetry(func() error {
		var createErr error
		connectorResponse, createErr = c.CreateConnector(req, true)
		return createErr
	}, d.Timeout(schema.TimeoutCreate))

	if err != nil {
		return err
	}

	log.Printf("[INFO] Created connector: %s", connectorResponse.Name)

	newConfFiltered := removeSecondKeysFromFirst(connectorResponse.Config, sensitiveCache)

	d.SetId(name)

	if err := d.Set("config_sensitive", sensitiveCache); err != nil {
		return err
	}

	if err := d.Set("config", newConfFiltered); err != nil {
		return err
	}

	return readWithRetry(d, meta, d.Timeout(schema.TimeoutCreate))
}

func connectorRead(d *schema.ResourceData, meta interface{}) error {
	c := meta.(kc.HighLevelClient)

	_, sensitiveCache := configFromRD(d)

	name := nameFromRD(d)

	req := kc.ConnectorRequest{
		Name: name,
	}

	log.Printf("[INFO] Reading connector: %s", name)

	conn, err := c.GetConnector(req)
	if err != nil {
		return err
	}

	if conn.Code == 404 {
		log.Printf("[WARN] Connector %s not found, removing from state", name)
		d.SetId("")
		return nil
	}

	newConfFiltered := removeSecondKeysFromFirst(conn.Config, sensitiveCache)

	if err := d.Set("config_sensitive", sensitiveCache); err != nil {
		return err
	}

	if err := d.Set("config", newConfFiltered); err != nil {
		return err
	}

	log.Printf("[INFO] Connector %s state refreshed", name)

	return nil
}

func connectorUpdate(d *schema.ResourceData, meta interface{}) error {
	c := meta.(kc.HighLevelClient)

	name := nameFromRD(d)

	config, sensitiveCache := configFromRD(d)

	if n, ok := config["name"]; ok && n != name {
		return errors.New("config.name must be identical to the resource name")
	} else if !ok {
		return errors.New("config.name is required and must be identical to the resource name")
	}

	req := kc.CreateConnectorRequest{
		ConnectorRequest: kc.ConnectorRequest{
			Name: name,
		},
		Config: config,
	}

	log.Printf("[INFO] Updating connector: %s", name)

	var conn kc.ConnectorResponse

	err := withRebalanceRetry(func() error {
		var updateErr error
		conn, updateErr = c.UpdateConnector(req, true)
		return updateErr
	}, d.Timeout(schema.TimeoutUpdate))

	if err != nil {
		return err
	}

	newConfFiltered := removeSecondKeysFromFirst(conn.Config, sensitiveCache)

	if err := d.Set("config", newConfFiltered); err != nil {
		return err
	}

	if err := d.Set("config_sensitive", sensitiveCache); err != nil {
		return err
	}

	return readWithRetry(d, meta, d.Timeout(schema.TimeoutUpdate))
}

func connectorDelete(d *schema.ResourceData, meta interface{}) error {
	c := meta.(kc.HighLevelClient)

	name := nameFromRD(d)

	req := kc.ConnectorRequest{
		Name: name,
	}

	log.Printf("[INFO] Deleting connector: %s", name)

	err := withRebalanceRetry(func() error {
		_, deleteErr := c.DeleteConnector(req, true)
		return deleteErr
	}, d.Timeout(schema.TimeoutDelete))

	if err != nil {
		return err
	}

	d.SetId("")

	return nil
}

func readWithRetry(d *schema.ResourceData, meta interface{}, timeout time.Duration) error {
	return withRebalanceRetry(func() error {
		return connectorRead(d, meta)
	}, timeout)
}

func withRebalanceRetry(fn func() error, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)

	backoff := 250 * time.Millisecond

	const maxBackoff = 5 * time.Second

	for {
		err := fn()
		if err == nil {
			return nil
		}

		if !isRebalanceError(err) {
			return err
		}

		if time.Now().After(deadline) {
			return fmt.Errorf("timed out waiting for Kafka Connect rebalance to finish: %w", err)
		}

		jitter := time.Duration(rand.Int63n(int64(backoff / 2)))
		sleep := backoff + jitter

		log.Printf(
			"[INFO] Kafka Connect rebalance in progress; retrying after %.2fs: %v",
			sleep.Seconds(),
			err,
		)

		time.Sleep(sleep)

		if backoff < maxBackoff {
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}
	}
}

func isRebalanceError(err error) bool {
	if err == nil {
		return false
	}

	msg := strings.ToLower(err.Error())

	return strings.Contains(msg, "rebalance") ||
		strings.Contains(msg, "rebalanceexpected") ||
		strings.Contains(msg, "rebalance is expected") ||
		strings.Contains(msg, "conflicting operation") ||
		strings.Contains(msg, "409")
}

func configFromRD(d *schema.ResourceData) (map[string]interface{}, map[string]interface{}) {
	cfg := mapFromRD(d, "config")
	sensitiveCfg := mapFromRD(d, "config_sensitive")

	config := combineMaps(cfg, sensitiveCfg)

	return config, sensitiveCfg
}

func nameFromRD(d *schema.ResourceData) string {
	return d.Get("name").(string)
}

func mapFromRD(d *schema.ResourceData, key string) map[string]interface{} {
	raw := d.Get(key)

	if raw == nil {
		return map[string]interface{}{}
	}

	result, ok := raw.(map[string]interface{})
	if !ok {
		return map[string]interface{}{}
	}

	return result
}

func combineMaps(first map[string]interface{}, second map[string]interface{}) map[string]interface{} {
	union := make(map[string]interface{})

	for k, v := range first {
		union[k] = v
	}

	for k, v := range second {
		union[k] = v
	}

	return union
}

func removeSecondKeysFromFirst(first map[string]interface{}, second map[string]interface{}) map[string]interface{} {
	result := make(map[string]interface{})

	for k, v := range first {
		result[k] = v
	}

	for k := range second {
		delete(result, k)
	}

	return result
}
