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

  backend "azurerm" {}
}

provider "azurerm" {
  features {}
  client_id       = var.azure_client_id
  subscription_id = var.azure_subscription_id
  tenant_id       = var.azure_tenant_id

  use_oidc      = var.use_oidc
  client_secret = var.use_oidc ? null : var.azure_client_secret

  storage_use_azuread = true
}

locals {
  data_lake_name                         = "dls${var.project_name}${terraform.workspace}"
  resource_group_name                    = "rg-${var.project_name}-${terraform.workspace}"
  resource_group_managed_name            = "rg-${var.project_name}-databricks-${terraform.workspace}"
  storage_account_managed_name           = "st${var.project_name}databricks${terraform.workspace}"
  databricks_workspace_name              = "dbw-${var.project_name}-${terraform.workspace}"
  databricks_catalog_name                = terraform.workspace
  key_vault_common_name                  = "kv-${var.project_name}-common"
  resource_group_common_name             = "rg-${var.project_name}-common"
  azure_databricks_access_connector_name = "dbac-${var.project_name}-${terraform.workspace}"
  virtual_network_name                   = "vnet-${var.project_name}-${terraform.workspace}"
  subnet_name_prefix                     = "snet-${var.project_name}-${terraform.workspace}"
  network_security_group_name            = "nsg-${var.project_name}-${terraform.workspace}"
  data_lake_private_endpoint_name        = "pe-${var.project_name}-dls-${terraform.workspace}"
  data_lake_network_interface_name       = "nic-${var.project_name}-dls-${terraform.workspace}"
  ip_rules                               = ["109.74.178.186"] #Office
  databricks_repo_path                   = "/Repos/${var.azure_client_id}/${terraform.workspace}"
  storage_account_common_name            = "st${var.project_name}common${terraform.workspace}"
}

module "resource_group_core" {
  source                = "../modules/resource-group-core"
  location              = var.location
  resource_group_name   = local.resource_group_name
  azure_client_id       = var.azure_client_id
  azure_subscription_id = var.azure_subscription_id
  azure_tenant_id       = var.azure_tenant_id
}

module "databricks_connector_core" {
  source                                 = "../modules/databricks-connector-core"
  location                               = module.resource_group_core.location
  resource_group_name                    = module.resource_group_core.name
  azure_client_id                        = var.azure_client_id
  azure_subscription_id                  = var.azure_subscription_id
  azure_tenant_id                        = var.azure_tenant_id
  azure_databricks_access_connector_name = local.azure_databricks_access_connector_name
}

module "data_lake_core" {
  source                              = "../modules/data-lake-core"
  location                            = module.resource_group_core.location
  data_lake_name                      = local.data_lake_name
  resource_group_name                 = module.resource_group_core.name
  ip_rules                            = local.ip_rules
  databricks_access_connector_id      = module.databricks_connector_core.databricks_access_connector_sp_id
  private_endpoint_name               = local.data_lake_private_endpoint_name
  network_interface_name              = local.data_lake_network_interface_name
  virtual_network_resource_group_name = module.virtual_network_core.resource_group_name
  virtual_network_id                  = module.virtual_network_core.id
  subnet_id                           = module.virtual_network_core.subnet_endpoints_id
  data_lake_containers                = var.medallion_layers
  azure_client_id                     = var.azure_client_id
  azure_subscription_id               = var.azure_subscription_id
  azure_tenant_id                     = var.azure_tenant_id
}

module "databricks_core" {
  source                                 = "../modules/databricks-core"
  location                               = var.location
  databricks_account_id                  = var.databricks_account_id
  databricks_workspace_name              = local.databricks_workspace_name
  resource_group_workspace_name          = module.resource_group_core.name
  resource_group_managed_name            = local.resource_group_managed_name
  databricks_sku                         = var.databricks_sku
  storage_account_managed_name           = local.storage_account_managed_name
  data_lake_name                         = local.data_lake_name
  key_vault_common_name                  = local.key_vault_common_name
  resource_group_common_name             = local.resource_group_common_name
  azure_databricks_access_connector_name = local.azure_databricks_access_connector_name
  databricks_catalog_name                = local.databricks_catalog_name
  virtual_network_id                     = module.virtual_network_core.id
  subnet_public_name                     = module.virtual_network_core.subnet_public_name
  subnet_private_name                    = module.virtual_network_core.subnet_private_name
  nsg_association_private_id             = module.virtual_network_core.nsg_association_private_id
  nsg_association_public_id              = module.virtual_network_core.nsg_association_public_id
  schemas                                = var.medallion_layers
  azure_client_id                        = var.azure_client_id
  azure_subscription_id                  = var.azure_subscription_id
  azure_tenant_id                        = var.azure_tenant_id
  access_connector_id                    = module.databricks_connector_core.databricks_access_connector_id
  databricks_repo_path                   = local.databricks_repo_path
}

module "virtual_network_core" {
  source                      = "../modules/virtual-network-core"
  location                    = module.resource_group_core.location
  resource_group_name         = module.resource_group_core.name
  virtual_network_name        = local.virtual_network_name
  subnet_name_prefix          = local.subnet_name_prefix
  network_security_group_name = local.network_security_group_name
  azure_client_id             = var.azure_client_id
  azure_subscription_id       = var.azure_subscription_id
  azure_tenant_id             = var.azure_tenant_id
}

module "storage_account_core" {
  source                  = "../modules/storage-account-core"
  resource_group_name     = module.resource_group_core.name
  location                = module.resource_group_core.location
  storage_account_name    = local.storage_account_common_name
  azure_client_id         = var.azure_client_id
  azure_subscription_id   = var.azure_subscription_id
  azure_tenant_id         = var.azure_tenant_id
  storage_container_names = ["dbt-manifest"]
  common_key_vault_id     = module.databricks_core.azure_common_key_vault_id
}
