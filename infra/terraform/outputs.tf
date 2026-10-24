output "eventhub_namespace_name" {
  value = azurerm_eventhub_namespace.main.name
}

output "eventhub_kafka_endpoint" {
  description = "Kafka-compatible bootstrap server for producers."
  value       = "${azurerm_eventhub_namespace.main.name}.servicebus.windows.net:9093"
}

output "eventhub_name" {
  value = azurerm_eventhub.telemetry.name
}

output "schema_group" {
  value = azurerm_eventhub_namespace_schema_group.contracts.name
}

output "adls_account_name" {
  value = azurerm_storage_account.lake.name
}
