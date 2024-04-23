terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.29.0"
    }

    azuread = {
      source  = "hashicorp/azuread"
      version = "~> 2.30.0"
    }
  }
}

provider "azurerm" {
  features {}
  use_oidc = true
  storage_use_azuread = true
  client_id = var.azure_client_id
  subscription_id = var.azure_subscription_id
  tenant_id = var.azure_tenant_id
}

locals {
    common_tags = {
    created_at = timestamp()
    env = terraform.workspace
    }
}


resource "azurerm_databricks_access_connector" "this" {
  name = var.azure_databricks_access_connector_name
  resource_group_name = var.resource_group_name
  location = var.location
  identity {
    type = "SystemAssigned"
  }
  tags = merge(local.common_tags)
  lifecycle { ignore_changes = [tags["created_at"]] }
}

output "databricks_access_connector_sp_id" {
  value = azurerm_databricks_access_connector.this.identity[0].principal_id
}

output "databricks_access_connector_id" {
  value = azurerm_databricks_access_connector.this.id
}