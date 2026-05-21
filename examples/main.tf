#########################################################
# Kafka Connect Provider - Basic Configuration
#########################################################

provider "kafka-connect" {

  # Kafka Connect REST API endpoint
  url = "http://localhost:8083"

  # Optional HTTP timeout in seconds
  timeout_seconds = 60
}

#########################################################
# JDBC Sink Connector Example
#########################################################

resource "kafka-connect_connector" "sqlite_sink" {

  # Connector name
  name = "sqlite-sink"

  #######################################################
  # Standard connector configuration
  #######################################################
  config = {

    # Connector name required by Kafka Connect
    "name" = "sqlite-sink"

    # JDBC Sink connector class
    "connector.class" = "io.confluent.connect.jdbc.JdbcSinkConnector"

    # Maximum number of connector tasks
    "tasks.max" = "1"

    # Source Kafka topic(s)
    "topics" = "orders"

    # JDBC database connection URL
    "connection.url" = "jdbc:sqlite:test.db"

    # Automatically create destination table
    "auto.create" = "true"

    # Database username
    "connection.user" = "admin"
  }

  #######################################################
  # Sensitive configuration
  #
  # Values defined here are masked in Terraform output
  #######################################################
  config_sensitive = {

    # Database password
    "connection.password" = "this-should-never-appear-unmasked"
  }

  #######################################################
  # Terraform operation timeouts
  #######################################################
  timeouts {
    create = "10m"
    update = "10m"
    delete = "5m"
  }
}

#########################################################
# Kafka Connect Provider - Basic Authentication
#########################################################

provider "kafka-connect" {

  # Provider alias
  alias = "with_basic_auth"

  # Kafka Connect REST API endpoint
  url = "http://localhost:8087"

  # Optional HTTP timeout in seconds
  timeout_seconds = 60

  #######################################################
  # HTTP Basic Authentication
  #######################################################
  basic_auth_username = "testuser"
  basic_auth_password = "testpassword"
}

#########################################################
# Connector Using Provider Alias
#########################################################

resource "kafka-connect_connector" "sqlite_sink_with_auth" {

  # Use provider with Basic Authentication enabled
  provider = kafka-connect.with_basic_auth

  # Connector name
  name = "sqlite-sink-with-auth"

  #######################################################
  # Standard connector configuration
  #######################################################
  config = {

    # Connector name
    "name" = "sqlite-sink-with-auth"

    # JDBC Sink connector implementation
    "connector.class" = "io.confluent.connect.jdbc.JdbcSinkConnector"

    # Maximum number of tasks
    "tasks.max" = "1"

    # Kafka topic(s)
    "topics" = "orders"

    # JDBC connection URL
    "connection.url" = "jdbc:sqlite:test.db"

    # Automatically create target table
    "auto.create" = "true"

    # Database username
    "connection.user" = "admin"
  }

  #######################################################
  # Sensitive configuration
  #######################################################
  config_sensitive = {

    # Database password stored securely
    "connection.password" = "this-should-never-appear-unmasked"
  }

  #######################################################
  # Terraform operation timeouts
  #######################################################
  timeouts {
    create = "10m"
    update = "10m"
    delete = "5m"
  }
}