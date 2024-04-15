terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~>3.0"
    }

    azuread = {
      source  = "hashicorp/azuread"
      version = "~> 2.30.0"
    }

  }

  backend "azurerm" {
    resource_group_name  = "rg-chefdp-tfstate"
    storage_account_name = "stchefdptfstate"
    container_name       = "terraform-state"
    key                  = "terraform.tfstate"
  }
}

provider "azurerm" {
  features {}
  storage_use_azuread = true
}


locals {
  resource_group_name = "rg-${var.project_name}-${var.environment}"
  key_vault_name = "kv-${var.project_name}-${var.environment}"

  ip_rules = [
     "109.74.178.186/32" #Office
  ]
  
  common_tags                = {
    created_at = timestamp()
    env = terraform.workspace
  }
}


data "azuread_client_config" "current" {}
data "azurerm_client_config" "current" {}
data "azuread_service_principal" "sc" {
  display_name = var.databricks_service_principal_name
}

resource "azurerm_resource_group" "this" {
  name     = local.resource_group_name
  location = var.location
}

resource "azurerm_key_vault" "key_vault" {
  name = local.key_vault_name
  location = azurerm_resource_group.this.location
  resource_group_name = azurerm_resource_group.this.name
  tenant_id = data.azuread_client_config.current.tenant_id
  soft_delete_retention_days = 90
  sku_name = "standard"

  tags = merge(local.common_tags)
  lifecycle { ignore_changes = [tags["created_at"]] }

  network_acls {
    default_action = "Deny"
    bypass = "AzureServices"
    ip_rules = local.ip_rules
  }
  
}

resource "azurerm_key_vault_access_policy" "this" {
  key_vault_id = azurerm_key_vault.key_vault.id
  tenant_id = data.azurerm_client_config.current.tenant_id
  object_id = data.azuread_client_config.current.object_id
  secret_permissions = ["List", "Set", "Get", "Delete", "Purge"]
  key_permissions = ["List"]
}

resource "azurerm_key_vault_secret" "client_id" {
  name          = "clientId"
  value         = data.azuread_service_principal.sc.application_id
  key_vault_id  = azurerm_key_vault.key_vault.id
}

resource "azurerm_key_vault_secret" "client_secret" {
  name          = "clientSecret"
  value         = data.azuread_service_principal.sc.application_id #The actual value must be added manually.
  key_vault_id  = azurerm_key_vault.key_vault.id
  lifecycle {
    ignore_changes = all
  }
}
