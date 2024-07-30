terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.29.0"
    }

    databricks = {
      source = "databricks/databricks"
      version = ">= 1.45.0"
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
  url = format("abfss://%s@%s.dfs.core.windows.net/",
    each.key,
    var.data_lake_name
  )
  credential_name = databricks_storage_credential.this.id
  comment = "Managed by Terraform"
  skip_validation = true
  owner = "location-owners"
}

resource "databricks_grants" "external_location" {
  for_each = toset(var.schemas)
  external_location = databricks_external_location.this[each.key].id
  grant {
    principal = var.azure_client_id
    privileges = ["ALL_PRIVILEGES"]
  }
  # Only add this grant if environment is dev
  dynamic "grant" {
    for_each = terraform.workspace == "dev" ? [1] : []
    content {
      principal  = "data-scientists"
      privileges = ["ALL_PRIVILEGES"]
    }
  }
  dynamic "grant" {
    for_each = terraform.workspace == "dev" ? [1] : []
    content {
      principal  = "data-engineers"
      privileges = ["ALL_PRIVILEGES"]
    }
  }
}

resource "databricks_grants" "storage_credential" {
  storage_credential = databricks_storage_credential.this.id
  grant {
    principal = var.azure_client_id
    privileges = ["ALL_PRIVILEGES"]
  }
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

resource "databricks_sql_endpoint" "db_wh_dbt" {
  name             = "dbt SQL Warehouse"
  cluster_size     = var.databricks_sql_warehouse_dbt_cluster_size
  min_num_clusters = var.databricks_sql_warehouse_dbt_min_num_clusters
  max_num_clusters = var.databricks_sql_warehouse_dbt_max_num_clusters
  auto_stop_mins   = var.databricks_sql_warehouse_auto_stop_mins
  enable_serverless_compute = true
  tags {
    custom_tags {
      key   = "user"
      value = "dbt developers"
    }
    custom_tags {
      key   = "tool"
      value = "dbt"
    }
    custom_tags {
      key   = "env"
      value = "${terraform.workspace}"
    }
    custom_tags {
      key = "managed_by"
      value = "terraform"
    }
  }
}

resource "databricks_sql_endpoint" "db_wh_explore" {
  name             = "Exploring SQL Warehouse"
  cluster_size     = var.databricks_sql_warehouse_explore_cluster_size
  min_num_clusters = var.databricks_sql_warehouse_explore_min_num_clusters
  max_num_clusters = var.databricks_sql_warehouse_explore_max_num_clusters
  auto_stop_mins   = var.databricks_sql_warehouse_auto_stop_mins
  enable_serverless_compute = true
  tags {
    custom_tags {
      key   = "user"
      value = "Explorers"
    }
    custom_tags {
      key   = "tool"
      value = "Databricks SQL Editor"
    }
    custom_tags {
      key   = "env"
      value = "${terraform.workspace}"
    }
    custom_tags {
      key = "managed_by"
      value = "terraform"
    }
  }
}

resource "time_rotating" "this" {
  rotation_days = 30
}

resource "databricks_token" "pat" {
  comment  = "Terraform (created: ${time_rotating.this.rfc3339})"
  # Token is valid for 60 days but is rotated after 30 days.
  # Run `terraform apply` within 60 days to refresh before it expires.
  lifetime_seconds = 60 * 24 * 60 * 60
}

data "azurerm_key_vault" "this" {
  name = var.key_vault_common_name
  resource_group_name = var.resource_group_common_name
}

resource "azurerm_key_vault_secret" "databricks_token" {
  name = "databricksToken-${terraform.workspace}"
  value = databricks_token.pat.token_value
  key_vault_id = data.azurerm_key_vault.this.id
  content_type = "Terraform (created: ${time_rotating.this.rfc3339}). Run `terraform apply` within 60 days to refresh before it expires."
  expiration_date = timeadd("${time_rotating.this.rfc3339}", "1440h")
}

resource "azurerm_key_vault_secret" "databricks_url" {
  name = "databricksUrl-${terraform.workspace}"
  value = azurerm_databricks_workspace.this.workspace_url
  key_vault_id = data.azurerm_key_vault.this.id
  content_type = "Managed by Terraform"
  expiration_date = "2050-01-01T00:00:00Z"
}


output "databricks_workspace_url" {
  value = azurerm_databricks_workspace.this.workspace_url
}

output "databricks_id" {
  value = azurerm_databricks_workspace.this.id
}
