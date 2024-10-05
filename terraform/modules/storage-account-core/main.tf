terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~>3.0"
    }
  }
}

provider "azurerm" {
  features {}
  use_oidc        = true
  client_id       = var.azure_client_id
  subscription_id = var.azure_subscription_id
  tenant_id       = var.azure_tenant_id
}

resource "azurerm_storage_account" "stchefdpcommon" {
  name                     = var.storage_account_name
  resource_group_name      = var.resource_group_name
  location                 = var.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  min_tls_version          = "TLS1_2"
}

resource "azurerm_storage_container" "containers" {
  count                 = length(var.storage_container_names)
  name                  = var.storage_container_names[count.index]
  storage_account_name  = azurerm_storage_account.stchefdpcommon.name
  container_access_type = "private"
}

resource "azurerm_key_vault_secret" "storage_account_key" {
  name         = "azure-storageaccount-${var.storage_account_name}-key"
  value        = azurerm_storage_account.stchefdpcommon.primary_access_key
  key_vault_id = var.common_key_vault_id
  content_type    = "Managed by Terraform. Access key for common storage account. Run `terraform apply` within 60 days to refresh before it expires."
  expiration_date = "2050-01-01T00:00:00Z"
  depends_on = [azurerm_storage_account.stchefdpcommon]
}
