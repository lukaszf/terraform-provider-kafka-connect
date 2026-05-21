package connect

import (
	"context"
	"crypto/tls"
	"log"
	"time"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"gopkg.in/resty.v1"

	kc "github.com/lukaszf/go-kafka-connect/v4/lib/connectors"
)

func Provider() *schema.Provider {
	log.Printf("[INFO] Creating Provider")
	provider := schema.Provider{
		Schema: map[string]*schema.Schema{
			"url": {
				Type:        schema.TypeString,
				Optional:    true,
				DefaultFunc: schema.EnvDefaultFunc("KAFKA_CONNECT_URL", ""),
			},
			"basic_auth_username": {
				Type:        schema.TypeString,
				Optional:    true,
				DefaultFunc: schema.EnvDefaultFunc("KAFKA_CONNECT_BASIC_AUTH_USERNAME", ""),
			},
			"basic_auth_password": {
				Type:        schema.TypeString,
				Optional:    true,
				DefaultFunc: schema.EnvDefaultFunc("KAFKA_CONNECT_BASIC_AUTH_PASSWORD", ""),
			},
			"tls_root_ca_file": {
				Type:        schema.TypeString,
				Optional:    true,
				DefaultFunc: schema.EnvDefaultFunc("KAFKA_CONNECT_TLS_ROOT_CA_FILE", ""),
			},
			"tls_auth_crt": {
				Type:        schema.TypeString,
				Optional:    true,
				DefaultFunc: schema.EnvDefaultFunc("KAFKA_CONNECT_TLS_AUTH_CRT", ""),
			},
			"tls_auth_key": {
				Type:        schema.TypeString,
				Optional:    true,
				DefaultFunc: schema.EnvDefaultFunc("KAFKA_CONNECT_TLS_AUTH_KEY", ""),
			},
			"tls_auth_is_insecure": {
				Type:        schema.TypeBool,
				Optional:    true,
				DefaultFunc: schema.EnvDefaultFunc("KAFKA_CONNECT_TLS_IS_INSECURE", false),
			},
			"headers": {
				Type: schema.TypeMap,
				Elem: &schema.Schema{
					Type: schema.TypeString,
				},
				Optional: true,
				// No DefaultFunc here to read from the env on account of this issue:
				// https://github.com/hashicorp/terraform-plugin-sdk/issues/142
			},
		},
		ConfigureContextFunc: providerConfigure,
		ResourcesMap: map[string]*schema.Resource{
			"kafka-connect_connector": kafkaConnectorResource(),
		},
	}
	log.Printf("[INFO] Created provider: %v", provider)
	return &provider
}

func providerConfigure(ctx context.Context, d *schema.ResourceData) (interface{}, diag.Diagnostics) {
	log.Printf("[INFO] Initializing KafkaConnect client")

	addr := d.Get("url").(string)
	log.Printf("[INFO] Kafka Connect URL from provider config: %q", addr)

	if addr == "" {
		return nil, diag.Errorf("Kafka Connect URL is empty. Set provider url or KAFKA_CONNECT_URL")
	}

	if !strings.HasPrefix(addr, "http://") && !strings.HasPrefix(addr, "https://") {
		return nil, diag.Errorf("Kafka Connect URL must start with http:// or https://, got: %s", addr)
	}

	c := kc.NewClient(addr, 60*time.Second)

	user := d.Get("basic_auth_username").(string)
	pass := d.Get("basic_auth_password").(string)

	if user != "" && pass != "" {
		c.SetBasicAuth(user, pass)
	}

	tlsRootCAFile := d.Get("tls_root_ca_file").(string)
	if tlsRootCAFile != "" {
		resty.SetRootCertificate(tlsRootCAFile)
	}

	crt := d.Get("tls_auth_crt").(string)
	key := d.Get("tls_auth_key").(string)
	isInsecure := d.Get("tls_auth_is_insecure").(bool)

	if isInsecure {
		c.SetInsecureSSL()
	}

	if crt != "" && key != "" {
		cert, err := tls.LoadX509KeyPair(crt, key)
		if err != nil {
			return nil, diag.Errorf("failed to load TLS client certificate/key: %s", err)
		}

		c.SetClientCertificates(cert)
	}

	headers := d.Get("headers").(map[string]interface{})
	for k, v := range headers {
		c.SetHeader(k, v.(string))
	}

	return c, nil
}
