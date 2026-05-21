package connect

import (
	"context"
	"crypto/tls"
	"log"
	"strings"
	"time"

	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"gopkg.in/resty.v1"

	kc "github.com/lukaszf/terraform-provider-kafka-connect/connect/lib/connectors"
)

func Provider() *schema.Provider {
	log.Printf("[INFO] Creating Provider")

	provider := schema.Provider{
		Schema: map[string]*schema.Schema{
			"url": {
				Type:        schema.TypeString,
				Optional:    true,
				DefaultFunc: schema.EnvDefaultFunc("KAFKA_CONNECT_URL", ""),
				Description: "Kafka Connect REST API URL",
			},

			"timeout_seconds": {
				Type:        schema.TypeInt,
				Optional:    true,
				DefaultFunc: schema.EnvDefaultFunc("KAFKA_CONNECT_TIMEOUT_SECONDS", 60),
				Description: "Kafka Connect HTTP client timeout in seconds",
			},

			"basic_auth_username": {
				Type:        schema.TypeString,
				Optional:    true,
				DefaultFunc: schema.EnvDefaultFunc("KAFKA_CONNECT_BASIC_AUTH_USERNAME", ""),
				Description: "Basic Auth username",
			},

			"basic_auth_password": {
				Type:        schema.TypeString,
				Optional:    true,
				Sensitive:   true,
				DefaultFunc: schema.EnvDefaultFunc("KAFKA_CONNECT_BASIC_AUTH_PASSWORD", ""),
				Description: "Basic Auth password",
			},

			"tls_root_ca_file": {
				Type:        schema.TypeString,
				Optional:    true,
				DefaultFunc: schema.EnvDefaultFunc("KAFKA_CONNECT_TLS_ROOT_CA_FILE", ""),
				Description: "Root CA certificate file",
			},

			"tls_auth_crt": {
				Type:        schema.TypeString,
				Optional:    true,
				DefaultFunc: schema.EnvDefaultFunc("KAFKA_CONNECT_TLS_AUTH_CRT", ""),
				Description: "TLS client certificate file",
			},

			"tls_auth_key": {
				Type:        schema.TypeString,
				Optional:    true,
				Sensitive:   true,
				DefaultFunc: schema.EnvDefaultFunc("KAFKA_CONNECT_TLS_AUTH_KEY", ""),
				Description: "TLS client private key file",
			},

			"tls_auth_is_insecure": {
				Type:        schema.TypeBool,
				Optional:    true,
				DefaultFunc: schema.EnvDefaultFunc("KAFKA_CONNECT_TLS_IS_INSECURE", false),
				Description: "Skip TLS certificate verification",
			},

			"headers": {
				Type: schema.TypeMap,
				Elem: &schema.Schema{
					Type: schema.TypeString,
				},
				Optional:    true,
				Description: "Additional HTTP headers",
			},
		},

		ConfigureContextFunc: providerConfigure,

		ResourcesMap: map[string]*schema.Resource{
			"kafka-connect_connector": kafkaConnectorResource(),
		},
	}

	log.Printf("[INFO] Provider created successfully")

	return &provider
}

func providerConfigure(ctx context.Context, d *schema.ResourceData) (interface{}, diag.Diagnostics) {
	log.Printf("[INFO] Initializing Kafka Connect client")

	addr := strings.TrimRight(
		strings.TrimSpace(d.Get("url").(string)),
		"/",
	)

	log.Printf("[INFO] Kafka Connect URL from provider config: %q", addr)

	if addr == "" {
		return nil, diag.Errorf(
			"Kafka Connect URL is empty. Set provider url or KAFKA_CONNECT_URL",
		)
	}

	if !strings.HasPrefix(addr, "http://") &&
		!strings.HasPrefix(addr, "https://") {
		return nil, diag.Errorf(
			"Kafka Connect URL must start with http:// or https://, got: %s",
			addr,
		)
	}

	timeoutSeconds := d.Get("timeout_seconds").(int)

	if timeoutSeconds <= 0 {
		timeoutSeconds = 60
	}

	timeout := time.Duration(timeoutSeconds) * time.Second

	log.Printf(
		"[INFO] Kafka Connect client timeout: %s",
		timeout.String(),
	)

	c := kc.NewClient(addr, timeout)

	user := strings.TrimSpace(
		d.Get("basic_auth_username").(string),
	)

	pass := d.Get("basic_auth_password").(string)

	if user != "" && pass != "" {
		log.Printf("[INFO] Configuring Basic Authentication")
		c.SetBasicAuth(user, pass)
	}

	tlsRootCAFile := strings.TrimSpace(
		d.Get("tls_root_ca_file").(string),
	)

	if tlsRootCAFile != "" {
		log.Printf("[INFO] Configuring Root CA: %s", tlsRootCAFile)
		resty.SetRootCertificate(tlsRootCAFile)
	}

	crt := strings.TrimSpace(
		d.Get("tls_auth_crt").(string),
	)

	key := strings.TrimSpace(
		d.Get("tls_auth_key").(string),
	)

	isInsecure := d.Get("tls_auth_is_insecure").(bool)

	log.Printf("[INFO] TLS insecure mode: %t", isInsecure)

	if isInsecure {
		c.SetInsecureSSL()
	}

	if crt != "" && key != "" {
		log.Printf("[INFO] Loading TLS client certificate")

		cert, err := tls.LoadX509KeyPair(crt, key)
		if err != nil {
			return nil, diag.Errorf(
				"failed to load TLS client certificate/key: %s",
				err,
			)
		}

		c.SetClientCertificates(cert)
	}

	headers := d.Get("headers").(map[string]interface{})

	for k, v := range headers {
		log.Printf("[DEBUG] Setting custom header: %s", k)
		c.SetHeader(k, v.(string))
	}

	log.Printf("[INFO] Kafka Connect client initialized successfully")

	return c, nil
}