terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 2.0"
    }
  }
}

provider "azurerm" {
  features {}
}

data "azuread_client_config" "current" {}
data "azurerm_client_config" "current" {}

resource "azurerm_resource_group" "setup" {
  name     = var.resource_group_name
  location = var.location
}

resource "azurerm_storage_account" "sa" {
  name                     = var.storage_account_name
  resource_group_name      = azurerm_resource_group.setup.name
  location                 = azurerm_resource_group.setup.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  blob_properties {
    versioning_enabled = true
    delete_retention_policy {
      days = 365
    }   
  }
}

resource "azurerm_storage_container" "ct" {
  name                 = "terraform-state"
  storage_account_name = azurerm_storage_account.sa.name
}


resource "azurerm_key_vault" "key_vault" {
  name = var.key_vault_name
  location = azurerm_resource_group.setup.location
  resource_group_name = azurerm_resource_group.setup.name
  tenant_id = data.azuread_client_config.current.tenant_id
  soft_delete_retention_days = 90
  sku_name = "standard"
  access_policy {
    tenant_id = data.azuread_client_config.current.tenant_id
    object_id = data.azuread_client_config.current.object_id

    key_permissions = [
      "List"
    ]

    secret_permissions = [
      "List", "Set", "Get", "Delete", "Purge"
    ]
  }
}


data "azurerm_storage_account" "sa" {
  name = azurerm_storage_account.sa.name
  resource_group_name = azurerm_resource_group.setup.name
}


resource "azurerm_key_vault_secret" "sub_id" {
  name          = "tfsSubscriptionId"
  value         = data.azurerm_client_config.current.subscription_id
  key_vault_id  = azurerm_key_vault.key_vault.id
}


resource "azurerm_key_vault_secret" "rg_name" {
  name          = "tfsResourceGroupName"
  value         = azurerm_resource_group.setup.name
  key_vault_id  = azurerm_key_vault.key_vault.id
}


resource "azurerm_key_vault_secret" "sa_name" {
  name          = "tfsStorageAccountName"
  value         = azurerm_storage_account.sa.name
  key_vault_id  = azurerm_key_vault.key_vault.id
}

resource "azurerm_key_vault_secret" "ct_name" {
  name          = "tfsStorageContainerName"
  value         = azurerm_storage_container.ct.name
  key_vault_id  = azurerm_key_vault.key_vault.id
}

resource "azurerm_key_vault_secret" "sa_key" {
  name          = "tfsStorageAccountKey"
  value         = azurerm_storage_account.sa.primary_access_key
  key_vault_id  = azurerm_key_vault.key_vault.id
}


output "account_id" {
  value = data.azuread_client_config.current.client_id
}

output "tenant_id" {
  value = data.azuread_client_config.current.tenant_id
}

output "object_id" {
  value = data.azuread_client_config.current.object_id
}