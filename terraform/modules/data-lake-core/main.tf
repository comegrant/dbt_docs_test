terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~>3.0"
    }

    databricks = {
      source = "databricks/databricks"
    }
  }
}

provider "azurerm" {
  features {}
  storage_use_azuread = true
  skip_provider_registration = true
}

provider "databricks" {
  host                        = azurerm_databricks_workspace.this.workspace_url
  azure_workspace_resource_id = azurerm_databricks_workspace.this.id
}

locals {
  common_tags = {
    created_at = timestamp()
    env = terraform.workspace
  }
}

resource "azurerm_storage_account" "this" {
  name                            = var.data_lake_name
  resource_group_name             = var.resource_group_name
  location                        = var.location
  account_tier                    = "Standard"
  account_replication_type        = "LRS"
  enable_https_traffic_only       = true
  min_tls_version                 = "TLS1_2"
  public_network_access_enabled   = true
  is_hns_enabled                  = true
  allow_nested_items_to_be_public = false
  shared_access_key_enabled       = false

  blob_properties {
    delete_retention_policy {
      days = 7
    }

    container_delete_retention_policy {
      days = 7
    }
  }

  identity {
    type = "SystemAssigned"
  }

  network_rules {
    default_action = "Deny"
    ip_rules = var.ip_rules
    bypass = ["AzureServices"]
  }

  tags = merge(local.common_tags)
  lifecycle { ignore_changes = [tags["created_at"]] }
}


resource "azurerm_role_assignment" "blob_data_contributor" {
  scope                = azurerm_storage_account.this.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = var.databricks_access_connector_id
}

resource "azurerm_role_assignment" "queue_data_contributor" {
  scope                = azurerm_storage_account.this.id
  role_definition_name = "Storage Queue Data Contributor"
  principal_id         = var.databricks_access_connector_id
}

resource "azurerm_storage_container" "this" {
  for_each              = toset(var.data_lake_containers)
  name = each.key
  storage_account_name = azurerm_storage_account.this.name
  container_access_type = "private"
}

resource "azurerm_private_endpoint" "this" {
  name                          = var.private_endpoint_name
  location                      = azurerm_storage_account.this.location
  resource_group_name           = azurerm_storage_account.this.resource_group_name
  subnet_id                     = var.subnet_id
  custom_network_interface_name = var.network_interface_name

  private_service_connection {
    name                           = azurerm_storage_account.this.name
    private_connection_resource_id = azurerm_storage_account.this.id
    subresource_names              = ["dfs"]
    is_manual_connection           = false
  }

  private_dns_zone_group {
    name                 = "default"
    private_dns_zone_ids = [azurerm_private_dns_zone.this.id]
  }
}

resource "azurerm_private_dns_zone" "this" {
  name                = "privatelink.dfs.core.windows.net"
  resource_group_name = var.virtual_network_resource_group_name
}

resource "azurerm_private_dns_zone_virtual_network_link" "this" {
  name                  = var.private_endpoint_name
  resource_group_name   = var.virtual_network_resource_group_name
  private_dns_zone_name = azurerm_private_dns_zone.this.name
  virtual_network_id    = var.virtual_network_id
}