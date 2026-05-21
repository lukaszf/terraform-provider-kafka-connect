# Terraform Provider Kafka Connect

[![Terraform Registry](https://img.shields.io/badge/dynamic/json?color=blue&label=terraform%20registry&query=%24.version&url=https%3A%2F%2Fregistry.terraform.io%2Fv1%2Fproviders%2Flukaszf%2Fkafka-connect)](https://registry.terraform.io/providers/lukaszf/kafka-connect/latest)
[![GitHub Release](https://img.shields.io/github/v/release/lukaszf/terraform-provider-kafka-connect)](https://github.com/lukaszf/terraform-provider-kafka-connect/releases/latest)
[![License](https://img.shields.io/github/license/lukaszf/terraform-provider-kafka-connect)](https://github.com/lukaszf/terraform-provider-kafka-connect/blob/main/LICENSE)

Terraform provider for managing Apache Kafka Connect connectors and configurations declaratively using the Kafka Connect REST API.

The provider is designed for:
- Self-managed Kafka Connect clusters
- Confluent Platform
- Enterprise CI/CD pipelines
- GitOps workflows
- Kubernetes environments
- Secure Kafka Connect automation

---

# Features

- Create and manage Kafka Connect connectors
- Declarative connector configuration management
- Sensitive configuration support
- Provider-level configurable HTTP timeout
- Resource-level Terraform operation timeouts
- Basic Authentication support
- TLS / mTLS support
- Custom HTTP headers support
- Detailed request/response logging
- Enterprise-ready deployment support

---

# Installation

## Terraform Registry

```hcl
terraform {
  required_providers {
    kafka-connect = {
      source  = "lukaszf/kafka-connect"
      version = "~> 1.0"
    }
  }
}
```

Terraform automatically downloads the provider from the Terraform Registry.

Terraform Registry:

https://registry.terraform.io/providers/lukaszf/kafka-connect/latest

---

## Manual Installation

You can manually download the latest release from GitHub:

https://github.com/lukaszf/terraform-provider-kafka-connect/releases/latest

Extract the binary into:

```bash
~/.terraform.d/plugins/
```

This installation method is useful for:
- Offline environments
- Air-gapped environments
- Restricted enterprise environments
- Internal CI/CD platforms

---

# Provider Configuration

The provider can be configured directly in Terraform or using environment variables.

---

## Example Provider Configuration

```hcl
terraform {
  required_providers {
    kafka-connect = {
      source  = "lukaszf/kafka-connect"
      version = "~> 1.0"
    }
  }
}

provider "kafka-connect" {
  url = var.kafka_connect_endpoint

  # HTTP client timeout in seconds
  # Controls how long the provider waits for Kafka Connect REST API responses
  timeout_seconds = 60

  # Optional Basic Authentication
  basic_auth_username = "user"
  basic_auth_password = "password"

  # Optional TLS / mTLS
  tls_auth_crt         = "/tmp/client.crt"
  tls_auth_key         = "/tmp/client.key"
  tls_auth_is_insecure = true

  # Optional Additional Headers
  headers = {
    "X-Environment" = "dev"
  }
}
```

---

# Provider Timeout

The `timeout_seconds` property configures the HTTP client timeout used by the provider when communicating with the Kafka Connect REST API.

Example:

```hcl
provider "kafka-connect" {
  url             = var.kafka_connect_endpoint
  timeout_seconds = 60
}
```

This timeout controls:
- HTTP request timeout
- REST API communication timeout
- Network waiting time
- Kafka Connect API responsiveness

Examples of affected operations:
- `POST /connectors`
- `GET /connectors/{name}/status`
- `PUT /connectors/{name}/config`
- `DELETE /connectors/{name}`

Typical use cases:
- Slow Kafka Connect REST API
- Enterprise proxy environments
- Kubernetes ingress/load balancers
- High-latency networks
- TLS/mTLS communication overhead

Default value:

```hcl
timeout_seconds = 60
```

---

# Connector Example

```hcl
resource "kafka-connect_connector" "sqlite_sink" {
  name = "sqlite-sink"

  config = {
    "name"            = "sqlite-sink"
    "connector.class" = "io.confluent.connect.jdbc.JdbcSinkConnector"
    "tasks.max"       = "1"
    "topics"          = "orders"
    "connection.url"  = "jdbc:sqlite:test.db"
    "auto.create"     = "true"
    "connection.user" = "admin"
  }

  config_sensitive = {
    "connection.password" = "super-secret-password"
  }

  timeouts {
    create = "10m"
    update = "10m"
    delete = "5m"
  }
}
```

---

# Resource Timeouts

The `timeouts {}` block controls how long Terraform waits for the entire resource lifecycle operation to complete.

Example:

```hcl
resource "kafka-connect_connector" "example" {
  name = "jdbc-source"

  config = {
    "connector.class" = "io.confluent.connect.jdbc.JdbcSourceConnector"
  }

  timeouts {
    create = "10m"
    update = "10m"
    delete = "5m"
  }
}
```

This timeout controls:
- Full Terraform resource operations
- Connector startup waiting time
- Connector deletion waiting time
- Retries and polling
- Kafka Connect rebalance waiting time
- Long-running connector initialization

Typical use cases:
- JDBC connectors with slow startup
- Large Kafka Connect clusters
- Slow external systems/databases
- Enterprise deployments
- Connectors requiring long initialization

Default values:

| Operation | Default Timeout |
|---|---|
| Create | `60s` |
| Update | `60s` |
| Delete | `60s` |

---

# Difference Between `timeout_seconds` and `timeouts {}`

The provider supports two different timeout mechanisms.

These timeouts operate at different layers.

| Property | Purpose | Scope |
|---|---|---|
| `timeout_seconds` | HTTP request timeout | Single REST API request |
| `timeouts {}` | Terraform operation timeout | Entire resource lifecycle operation |

---

## Example

```hcl
provider "kafka-connect" {
  url             = var.kafka_connect_endpoint
  timeout_seconds = 60
}

resource "kafka-connect_connector" "jdbc" {
  name = "jdbc-source"

  config = {
    "connector.class" = "io.confluent.connect.jdbc.JdbcSourceConnector"
  }

  timeouts {
    create = "10m"
    update = "10m"
    delete = "5m"
  }
}
```

Terraform behavior:

1. Terraform sends HTTP requests to Kafka Connect
2. Each REST API request can take up to `60 seconds`
3. Terraform continuously polls connector status
4. Entire connector creation can take up to `10 minutes`

This means:
- A single HTTP request cannot hang forever
- Terraform still supports long-running connector startup processes
- Connector initialization remains stable in enterprise environments

---

# Environment Variables

The provider supports configuration through environment variables.

| Terraform Property | Environment Variable |
|---|---|
| `url` | `KAFKA_CONNECT_URL` |
| `timeout_seconds` | `KAFKA_CONNECT_TIMEOUT_SECONDS` |
| `basic_auth_username` | `KAFKA_CONNECT_BASIC_AUTH_USERNAME` |
| `basic_auth_password` | `KAFKA_CONNECT_BASIC_AUTH_PASSWORD` |
| `tls_auth_crt` | `KAFKA_CONNECT_TLS_AUTH_CRT` |
| `tls_auth_key` | `KAFKA_CONNECT_TLS_AUTH_KEY` |
| `tls_auth_is_insecure` | `KAFKA_CONNECT_TLS_IS_INSECURE` |

---

# Provider Properties

| Property | Type | Description |
|---|---|---|
| `url` | String | Kafka Connect REST API URL |
| `timeout_seconds` | Number | HTTP client timeout in seconds |
| `basic_auth_username` | String | Username for Basic Authentication |
| `basic_auth_password` | String | Password for Basic Authentication |
| `tls_auth_crt` | String | TLS client certificate path |
| `tls_auth_key` | String | TLS client private key path |
| `tls_auth_is_insecure` | Bool | Skip TLS certificate verification |
| `headers` | Map(String) | Additional HTTP headers |

---

# Resource Properties

## kafka-connect_connector

| Property | Type | Description |
|---|---|---|
| `name` | String | Connector name |
| `config` | Map(String) | Connector configuration |
| `config_sensitive` | Map(String) | Sensitive connector configuration |
| `timeouts` | Block | Custom create/update/delete timeouts |

---

# Logging

The provider outputs detailed HTTP request and response logs during:
- Create
- Read
- Update
- Delete

This is useful for:
- Debugging Kafka Connect API issues
- Troubleshooting enterprise environments
- CI/CD diagnostics
- Long-running connector operations

---

# Recommended Enterprise Configuration

```hcl
provider "kafka-connect" {
  url             = var.kafka_connect_endpoint
  timeout_seconds = 60
}

resource "kafka-connect_connector" "example" {
  name = "example"

  config = {
    "connector.class" = "FileStreamSource"
  }

  timeouts {
    create = "10m"
    update = "10m"
    delete = "5m"
  }
}
```

This configuration provides:
- Stable REST API communication
- Better CI/CD reliability
- Better Kubernetes compatibility
- Better handling of Kafka Connect rebalance operations
- Improved support for slow enterprise systems

---

# Development

## Requirements

- Go 1.24+
- Terraform
- Kafka Connect cluster

---

## Clone Repository

```bash
git clone https://github.com/lukaszf/terraform-provider-kafka-connect.git

cd terraform-provider-kafka-connect
```

---

## Build Provider

```bash
make build
```

---

## Run Unit Tests

```bash
make test
```

---

## Run Acceptance Tests

```bash
make testacc
```

---

# Local Development Installation

To build and install the provider locally:

```bash
make install
```

Terraform will then use the locally compiled provider binary.

---

# Release Process

To create a new release:

```bash
git tag v1.0.0
git push origin v1.0.0
```

GitHub Actions automatically:
- Build provider binaries
- Generate SHA256 checksums
- Sign artifacts using GPG
- Publish GitHub Releases
- Publish to Terraform Registry

---

# Supported Authentication Methods

| Authentication | Supported |
|---|---|
| No Authentication | Yes |
| Basic Authentication | Yes |
| TLS | Yes |
| mTLS | Yes |

---

# Supported Platforms

| OS | Architecture |
|---|---|
| Linux | amd64 / arm64 |
| macOS | amd64 / arm64 |
| Windows | amd64 |

---

# Roadmap

Planned features:
- Connector pause/resume support
- Connector restart support
- Kafka Connect plugin management
- Connector status data source
- OAuth/OIDC authentication
- Improved drift detection

---

# Contributing

Contributions are welcome.

Please open issues and pull requests on GitHub:

https://github.com/lukaszf/terraform-provider-kafka-connect

---

# License

MIT License

Copyright (c) 2018 Conor Mongey  
Copyright (c) 2026 Łukasz Fedorowiat