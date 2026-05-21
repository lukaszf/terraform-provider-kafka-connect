package connect

import (
	"os"
	"testing"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

var testProvider *schema.Provider
var testProviders map[string]*schema.Provider

func init() {
	testProvider = Provider()

	testProviders = map[string]*schema.Provider{
		"kafka-connect": testProvider,
	}
}

func TestProvider(t *testing.T) {
	provider := Provider()

	if err := provider.InternalValidate(); err != nil {
		t.Fatalf("provider validation failed: %s", err)
	}
}

func TestProviderSchema(t *testing.T) {
	provider := Provider()

	requiredProviderFields := []string{
		"url",
		"timeout_seconds",
		"basic_auth_username",
		"basic_auth_password",
		"tls_root_ca_file",
		"tls_auth_crt",
		"tls_auth_key",
		"tls_auth_is_insecure",
		"headers",
	}

	for _, field := range requiredProviderFields {
		if _, ok := provider.Schema[field]; !ok {
			t.Fatalf("expected provider schema field %q to exist", field)
		}
	}

	if provider.Schema["basic_auth_password"].Sensitive != true {
		t.Fatalf("expected basic_auth_password to be sensitive")
	}

	if provider.Schema["tls_auth_key"].Sensitive != true {
		t.Fatalf("expected tls_auth_key to be sensitive")
	}
}

func TestProviderResources(t *testing.T) {
	provider := Provider()

	if _, ok := provider.ResourcesMap["kafka-connect_connector"]; !ok {
		t.Fatalf("expected kafka-connect_connector resource to be registered")
	}
}

func testAccPreCheck(t *testing.T) {
	connectVar := "KAFKA_CONNECT_URL"

	if value := os.Getenv(connectVar); value == "" {
		t.Fatalf("%s env var must be set for acceptance tests", connectVar)
	}
}
