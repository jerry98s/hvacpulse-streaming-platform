terraform {
  required_version = ">= 1.6"
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.0"
    }
  }
}

provider "azurerm" {
  features {}
}

resource "azurerm_resource_group" "main" {
  name     = "rg-${var.prefix}"
  location = var.location
  tags     = var.tags
}

# --- Event Hubs: Kafka-compatible ingestion endpoint -----------------------

resource "azurerm_eventhub_namespace" "main" {
  name                = "evhns-${var.prefix}"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  sku                 = "Standard"
  capacity            = 1
  tags                = var.tags
}

resource "azurerm_eventhub" "telemetry" {
  name              = "hvac-telemetry"
  namespace_id      = azurerm_eventhub_namespace.main.id
  partition_count   = var.eventhub_partition_count
  message_retention = var.eventhub_message_retention_days
}

resource "azurerm_eventhub_namespace_schema_group" "contracts" {
  name                 = "hvacpulse-contracts"
  namespace_id         = azurerm_eventhub_namespace.main.id
  schema_compatibility = "Backward"
  schema_type          = "Avro"
}

# --- ADLS Gen2: Delta Bronze/Silver/Gold storage ---------------------------

resource "azurerm_storage_account" "lake" {
  # Storage account names: 3-24 chars, lowercase alphanumeric, globally unique.
  name                     = substr("st${replace(var.prefix, "-", "")}${random_string.suffix.result}", 0, 24)
  resource_group_name      = azurerm_resource_group.main.name
  location                 = azurerm_resource_group.main.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  is_hns_enabled           = true
  tags                     = var.tags
}

resource "random_string" "suffix" {
  length  = 6
  special = false
  upper   = false
}

resource "azurerm_storage_container" "bronze" {
  name               = "bronze"
  storage_account_id = azurerm_storage_account.lake.id
}

resource "azurerm_storage_container" "silver" {
  name               = "silver"
  storage_account_id = azurerm_storage_account.lake.id
}

resource "azurerm_storage_container" "gold" {
  name               = "gold"
  storage_account_id = azurerm_storage_account.lake.id
}
