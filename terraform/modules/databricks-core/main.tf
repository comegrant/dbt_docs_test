terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.29.0"
    }

    databricks = {
      source = "databricks/databricks"
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

provider "databricks" {
  host                        = azurerm_databricks_workspace.this.workspace_url
  azure_workspace_resource_id = azurerm_databricks_workspace.this.id
}

resource "azurerm_databricks_workspace" "this" {
  name                        = var.databricks_workspace_name
  resource_group_name         = var.resource_group_workspace_name
  location                    = var.location
  sku                         = var.databricks_sku
  managed_resource_group_name = var.resource_group_managed_name
  
  custom_parameters {
    virtual_network_id                                    = var.virtual_network_id
    private_subnet_name                                   = var.subnet_private_name
    public_subnet_name                                    = var.subnet_public_name
    private_subnet_network_security_group_association_id  = var.nsg_association_private_id
    public_subnet_network_security_group_association_id   = var.nsg_association_public_id
    storage_account_name                                  = var.storage_account_managed_name
  }

  tags = merge(local.common_tags)
  lifecycle { ignore_changes = [tags["created_at"]] }
  
}

resource "databricks_workspace_conf" "this" {
  custom_config = {
    "enableDcs": true
  }
}

resource "databricks_storage_credential" "this" {
  name = var.azure_databricks_access_connector_name
  azure_managed_identity {
    access_connector_id = var.access_connector_id 
  }
  comment = "Managed identy credential using the databricks access connector, managed by Terraform"
}


resource "databricks_external_location" "this" {
  for_each = toset(var.schemas)
  name = "delta_lake_${terraform.workspace}_${each.key}"
  url = format("abfss://%s@%s.dfs.core.windows.net",
    each.key,
    var.data_lake_name
  )
  credential_name = databricks_storage_credential.this.id
  comment = "Managed by Terraform"
}

resource "databricks_schema" "this" {
  for_each = toset(var.schemas)
  catalog_name = var.databricks_catalog_name
  name = each.key
  comment = "Managed by Terraform"
  storage_root = databricks_external_location.this[each.key].url
}


data "azurerm_client_config" "current" {}
resource "databricks_secret_scope" "auth_common" {
  name                     = "auth_common"
  initial_manage_principal = "users"
  keyvault_metadata {
    resource_id = "/subscriptions/${data.azurerm_client_config.current.subscription_id}/resourceGroups/${var.resource_group_common_name}/providers/Microsoft.KeyVault/vaults/${var.key_vault_common_name}"
    dns_name = "https://${var.key_vault_common_name}.vault.azure.net/"
  }
}

resource "databricks_instance_pool" "this" {
  instance_pool_name = "pool"
  min_idle_instances = var.databricks_pool_min_idle
  max_capacity = var.databricks_pool_max_capacity
  node_type_id = var.databricks_node_type
  idle_instance_autotermination_minutes = var.databricks_pool_idle_termination
}

resource "databricks_cluster" "this" {
  cluster_name = "cluster"
  spark_version = var.databricks_spark_version
  instance_pool_id = databricks_instance_pool.this.id
  driver_instance_pool_id = databricks_instance_pool.this.id
  autotermination_minutes = var.databricks_cluster_idle_termination
  data_security_mode = "USER_ISOLATION"
  autoscale {
    min_workers = var.databricks_cluster_min_workers
    max_workers = var.databricks_cluster_max_workers
  }
}

resource "databricks_sql_endpoint" "this" {
  name             = "serverless sql wh"
  cluster_size     = var.databricks_serverless_sql_cluster_size
  min_num_clusters = var.databricks_serverless_sql_min_num_clusters
  max_num_clusters = var.databricks_serverless_sql_max_num_clusters
  auto_stop_mins   = var.databricks_serverless_sql_idle_termination
  enable_serverless_compute = true
  tags {
    custom_tags {
      key   = "Users"
      value = "Data Team Serverless SQL"
    }
  }
}





output "databricks_workspace_url" {
  value = azurerm_databricks_workspace.this.workspace_url
}

output "databricks_id" {
  value = azurerm_databricks_workspace.this.id
}
