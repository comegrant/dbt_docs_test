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

variable "azure_client_id" {}
variable "azure_subscription_id" {}
variable "azure_tenant_id" {}

provider "azurerm" {
  features {}
  use_oidc        = true
  client_id       = var.azure_client_id
  subscription_id = var.azure_subscription_id
  tenant_id       = var.azure_tenant_id
}

resource "azurerm_resource_group" "this" {
  name     = var.resource_group_name
  location = var.location
}

output "name" {
  value = azurerm_resource_group.this.name
}

output "id" {
  value = azurerm_resource_group.this.id
}

output "location" {
  value = azurerm_resource_group.this.location
}