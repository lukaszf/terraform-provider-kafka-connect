package main

import (
	c "github.com/lukaszf/terraform-provider-kafka-connect/connect"
	"github.com/hashicorp/terraform-plugin-sdk/v2/plugin"
)

func main() {
	plugin.Serve(&plugin.ServeOpts{ProviderFunc: c.Provider})
}
