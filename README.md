# Terraform Provider Kafka Connect

A Terraform provider for managing Apache Kafka Connect connectors and configurations.

This provider allows you to:
- Create and manage Kafka Connect connectors
- Configure connector settings
- Manage sensitive connector configuration securely
- Configure custom timeouts for connector operations
- Use Basic Authentication and TLS/mTLS
- Manage Kafka Connect declaratively with Terraform

---

# Installation

```hcl
terraform {
  required_providers {
    kafka-connect = {
      source  = "lukaszf/kafka-connect"
      version = "1.0.0"
    }
  }
}
```

Terraform will automatically download the provider from the Terraform Registry.

You can also manually download the latest release from:

https://github.com/lukaszf/terraform-provider-kafka-connect/releases/latest

and extract it into your Terraform plugin directory:

```bash
~/.terraform.d/plugins/
```

This method is especially useful for:
- Offline environments
- Air-gapped environments
- Restricted enterprise environments

---

# Example

Configure the provider directly or use environment variables.

## Provider Configuration

```hcl
terraform {
  required_providers {
    kafka-connect = {
      source  = "lukaszf/kafka-connect"
      version = "1.0.0"
    }
  }
}

provider "kafka-connect" {
  url = "http://localhost:8083"

  # Optional Basic Authentication
  basic_auth_username = "user"
  basic_auth_password = "password"

  # Optional TLS / mTLS
  tls_auth_crt         = "/tmp/cert.pem"
  tls_auth_key         = "/tmp/key.pem"
  tls_auth_is_insecure = true
}
```

## Connector Example

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
    "connection.password" = "this-should-never-appear-unmasked"
  }

  timeouts {
    create = "10m"
    update = "10m"
    delete = "5m"
  }
}
```

---

# Environment Variables

The provider can also be configured using environment variables.

| Property | Environment Variable |
|---|---|
| `url` | `KAFKA_CONNECT_URL` |
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

# Timeouts

The `kafka-connect_connector` resource supports configurable timeouts.

```hcl
resource "kafka-connect_connector" "example" {
  name = "my-connector"

  config = {
    "connector.class" = "FileStreamSink"
  }

  timeouts {
    create = "10m"
    update = "10m"
    delete = "5m"
  }
}
```

Default timeout values:
- Create: `60s`
- Update: `60s`
- Delete: `60s`

---

# Development

## Requirements

- Go 1.24+
- Terraform
- Kafka Connect cluster

## Clone Repository

```bash
git clone https://github.com/lukaszf/terraform-provider-kafka-connect.git

cd terraform-provider-kafka-connect
```

## Build Provider

```bash
make build
```

## Run Tests

```bash
make test
```

## Run Acceptance Tests

```bash
make testacc
```

---

# Release

To create a new release:

```bash
git tag v1.0.0
git push origin v1.0.0
```

GitHub Actions will automatically:
- Build binaries
- Generate checksums
- Sign artifacts using GPG
- Create a GitHub Release

---

# License

MIT License

Copyright (c) 2018 Conor Mongey  
Copyright (c) 2026 Łukasz Fedorowiat