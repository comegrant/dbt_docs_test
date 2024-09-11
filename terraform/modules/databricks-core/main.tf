terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.29.0"
    }

    databricks = {
      source  = "databricks/databricks"
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
  use_oidc            = true
  storage_use_azuread = true
  client_id           = var.azure_client_id
  subscription_id     = var.azure_subscription_id
  tenant_id           = var.azure_tenant_id
}

locals {
  common_tags = {
    created_at = timestamp()
    env        = terraform.workspace
  }
}

provider "databricks" {
  host                        = azurerm_databricks_workspace.this.workspace_url
  azure_workspace_resource_id = azurerm_databricks_workspace.this.id
}


##########################################
###     Workspace configuration        ###
##########################################

resource "azurerm_databricks_workspace" "this" {
  name                        = var.databricks_workspace_name
  resource_group_name         = var.resource_group_workspace_name
  location                    = var.location
  sku                         = var.databricks_sku
  managed_resource_group_name = var.resource_group_managed_name

  custom_parameters {
    virtual_network_id                                   = var.virtual_network_id
    private_subnet_name                                  = var.subnet_private_name
    public_subnet_name                                   = var.subnet_public_name
    private_subnet_network_security_group_association_id = var.nsg_association_private_id
    public_subnet_network_security_group_association_id  = var.nsg_association_public_id
    storage_account_name                                 = var.storage_account_managed_name
  }

  tags = merge(local.common_tags)
  lifecycle { ignore_changes = [tags["created_at"]] }

}

resource "databricks_workspace_conf" "this" {
  custom_config = {
    "enableDcs" : true
  }
}


####################################################
###   Catalogs, Schemas and external locations   ###
####################################################

resource "databricks_storage_credential" "this" {
  name = var.azure_databricks_access_connector_name
  azure_managed_identity {
    access_connector_id = var.access_connector_id
  }
  comment = "Managed identy credential using the databricks access connector, managed by Terraform"
}

resource "databricks_external_location" "this" {
  for_each = toset(var.schemas)
  name     = "delta_lake_${terraform.workspace}_${each.key}"
  url = format("abfss://%s@%s.dfs.core.windows.net/",
    each.key,
    var.data_lake_name
  )
  credential_name = databricks_storage_credential.this.id
  comment         = "Managed by Terraform"
  skip_validation = true
  owner           = "location-owners"
}

resource "databricks_external_location" "segment" {
  name     = "delta_lake_${terraform.workspace}_segment"
  url = format("abfss://%s@%s.dfs.core.windows.net/",
    "segment",
    var.data_lake_name
  )
  credential_name = databricks_storage_credential.this.id
  comment         = "Managed by Terraform"
  skip_validation = true
  owner           = "location-owners"
}

resource "databricks_schema" "this" {
  for_each     = toset(var.schemas)
  catalog_name = var.databricks_catalog_name
  name         = each.key
  comment      = "Managed by Terraform"
  storage_root = databricks_external_location.this[each.key].url
}

resource "databricks_catalog" "segment" {
  name = "segment_${terraform.workspace}"
  comment = "Managed by Terraform. This catalog is used for ingestion from Segment"
  properties = {
    purpose = "Segment Ingest"
  }
  storage_root = databricks_external_location.segment.url
  isolation_mode = "ISOLATED"
}



##########################################
###            Secret scope            ###
##########################################

data "azurerm_client_config" "current" {}

resource "databricks_secret_scope" "auth_common" {
  name                     = "auth_common"
  initial_manage_principal = "users"
  keyvault_metadata {
    resource_id = "/subscriptions/${data.azurerm_client_config.current.subscription_id}/resourceGroups/${var.resource_group_common_name}/providers/Microsoft.KeyVault/vaults/${var.key_vault_common_name}"
    dns_name    = "https://${var.key_vault_common_name}.vault.azure.net/"
  }
}


##########################################
###               Compute              ###
##########################################

resource "databricks_sql_endpoint" "db_wh_dbt" {
  name                      = "dbt SQL Warehouse"
  cluster_size              = var.databricks_sql_warehouse_dbt_cluster_size
  min_num_clusters          = var.databricks_sql_warehouse_dbt_min_num_clusters
  max_num_clusters          = var.databricks_sql_warehouse_dbt_max_num_clusters
  auto_stop_mins            = var.databricks_sql_warehouse_auto_stop_mins
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
      value = terraform.workspace
    }
    custom_tags {
      key   = "managed_by"
      value = "terraform"
    }
  }
}

resource "databricks_sql_endpoint" "db_wh_explore" {
  name                      = "Exploring SQL Warehouse"
  cluster_size              = var.databricks_sql_warehouse_explore_cluster_size
  min_num_clusters          = var.databricks_sql_warehouse_explore_min_num_clusters
  max_num_clusters          = var.databricks_sql_warehouse_explore_max_num_clusters
  auto_stop_mins            = var.databricks_sql_warehouse_auto_stop_mins
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
      value = terraform.workspace
    }
    custom_tags {
      key   = "managed_by"
      value = "terraform"
    }
  }
}

resource "databricks_sql_endpoint" "db_wh_powerbi" {
  name                      = "Power BI SQL Warehouse"
  cluster_size              = var.databricks_sql_warehouse_explore_cluster_size
  min_num_clusters          = var.databricks_sql_warehouse_explore_min_num_clusters
  max_num_clusters          = var.databricks_sql_warehouse_explore_max_num_clusters
  auto_stop_mins            = var.databricks_sql_warehouse_auto_stop_mins
  enable_serverless_compute = true
  tags {
    custom_tags {
      key   = "user"
      value = "Power BI users"
    }
    custom_tags {
      key   = "tool"
      value = "Power BI"
    }
    custom_tags {
      key   = "env"
      value = terraform.workspace
    }
    custom_tags {
      key   = "managed_by"
      value = "terraform"
    }
  }
}

resource "databricks_sql_endpoint" "db_wh_tableau" {
  name                      = "Tableau SQL Warehouse"
  cluster_size              = var.databricks_sql_warehouse_explore_cluster_size
  min_num_clusters          = var.databricks_sql_warehouse_explore_min_num_clusters
  max_num_clusters          = var.databricks_sql_warehouse_explore_max_num_clusters
  auto_stop_mins            = var.databricks_sql_warehouse_auto_stop_mins
  enable_serverless_compute = true
  tags {
    custom_tags {
      key   = "user"
      value = "Tableau users"
    }
    custom_tags {
      key   = "tool"
      value = "Tableau"
    }
    custom_tags {
      key   = "env"
      value = terraform.workspace
    }
    custom_tags {
      key   = "managed_by"
      value = "terraform"
    }
  }
}

resource "databricks_sql_endpoint" "db_wh_omni" {
  name                      = "Omni SQL Warehouse"
  cluster_size              = var.databricks_sql_warehouse_explore_cluster_size
  min_num_clusters          = var.databricks_sql_warehouse_explore_min_num_clusters
  max_num_clusters          = var.databricks_sql_warehouse_explore_max_num_clusters
  auto_stop_mins            = var.databricks_sql_warehouse_auto_stop_mins
  enable_serverless_compute = true
  tags {
    custom_tags {
      key   = "user"
      value = "Omni users"
    }
    custom_tags {
      key   = "tool"
      value = "Omni"
    }
    custom_tags {
      key   = "env"
      value = terraform.workspace
    }
    custom_tags {
      key   = "managed_by"
      value = "terraform"
    }
  }
}

resource "databricks_sql_endpoint" "db_wh_segment" {
  name                      = "Segment SQL Warehouse"
  cluster_size              = "Small"
  min_num_clusters          = 2
  max_num_clusters          = 6
  auto_stop_mins            = var.databricks_sql_warehouse_auto_stop_mins
  enable_serverless_compute = true
  tags {
    custom_tags {
      key   = "user"
      value = "Segment ingestion"
    }
    custom_tags {
      key   = "tool"
      value = "Segment"
    }
    custom_tags {
      key   = "env"
      value = terraform.workspace
    }
    custom_tags {
      key   = "managed_by"
      value = "terraform"
    }
  }
}

##########################################
### Preperation for Service Principals ###
##########################################

data "databricks_service_principal" "resource-sp" {
  application_id = var.azure_client_id
}

data "databricks_group" "admins" {
  display_name = "admins"
}

provider "databricks" {
  alias      = "accounts"
  host       = "https://accounts.azuredatabricks.net"
  account_id = var.databricks_account_id
}

#####################################
### Service Principal for Segment ###
#####################################

resource "databricks_service_principal" "segment_sp" {
  display_name = "segment_sp_${terraform.workspace}"
}

resource "databricks_service_principal_secret" "segment_sp" {
  provider             = databricks.accounts
  service_principal_id = databricks_service_principal.segment_sp.id
}

resource "databricks_group_member" "segment_sp_is_ws_admin" {
  group_id  = data.databricks_group.admins.id
  member_id = databricks_service_principal.segment_sp.id
}


#####################################
### Service Principal for Bundles ###
#####################################

resource "databricks_service_principal" "bundle_sp" {
  display_name = "bundle_sp_${terraform.workspace}"
}

resource "databricks_group_member" "bundle_sp_is_ws_admin" {
  group_id  = data.databricks_group.admins.id
  member_id = databricks_service_principal.bundle_sp.id
}

resource "databricks_service_principal_secret" "bundle_sp" {
  provider             = databricks.accounts
  service_principal_id = databricks_service_principal.bundle_sp.id
}

resource "databricks_access_control_rule_set" "automation_sp_bundle_rule_set" {
  provider = databricks.accounts
  name = "accounts/${var.databricks_account_id}/servicePrincipals/${databricks_service_principal.bundle_sp.application_id}/ruleSets/default"

  grant_rules {
    principals = [data.databricks_service_principal.resource-sp.acl_principal_id]
    role       = "roles/servicePrincipal.user"
  }

  grant_rules {
    principals = [data.databricks_service_principal.resource-sp.acl_principal_id]
    role       = "roles/servicePrincipal.manager"
  }
}

provider "databricks" {
  alias = "bundle-sp"
  auth_type = "oauth-m2m"
  host = azurerm_databricks_workspace.this.workspace_url
  client_id = databricks_service_principal.bundle_sp.application_id
  client_secret = databricks_service_principal_secret.bundle_sp.secret
}

resource "time_rotating" "this" {
  rotation_days = 30
}

resource "databricks_token" "pat_bundle_sp" {
  provider = databricks.bundle-sp
  comment  = "Terraform (created: ${time_rotating.this.rfc3339})"
  # Token is valid for 60 days but is rotated after 30 days.
  # Run `terraform apply` within 60 days to refresh before it expires.
  lifetime_seconds = 60 * 24 * 60 * 60
}

##########################################
###     Add secrets to key vault       ###
##########################################

data "azurerm_key_vault" "this" {
  name                = var.key_vault_common_name
  resource_group_name = var.resource_group_common_name
}

resource "azurerm_key_vault_secret" "databricks_url" {
  name            = "databricks-workspace-url-${terraform.workspace}"
  value           = azurerm_databricks_workspace.this.workspace_url
  key_vault_id    = data.azurerm_key_vault.this.id
  content_type    = "Managed by Terraform. Url for ${terraform.workspace} workspace in Databricks"
  expiration_date = "2050-01-01T00:00:00Z"
}

resource "azurerm_key_vault_secret" "bundle_sp_secret" {
  name            = "databricks-sp-bundle-secret-${terraform.workspace}"
  value           = databricks_service_principal_secret.bundle_sp.secret
  key_vault_id    = data.azurerm_key_vault.this.id
  content_type    = "Managed by Terraform. OAuth secret for bundle service principal. Run `terraform apply` within 60 days to refresh before it expires."
  expiration_date = "2050-01-01T00:00:00Z"
}

resource "azurerm_key_vault_secret" "bundle_sp_pat" {
  name            = "databricks-sp-bundle-pat-${terraform.workspace}"
  value           = databricks_token.pat_bundle_sp.token_value
  key_vault_id    = data.azurerm_key_vault.this.id
  content_type    = "Managed by Terraform. Personal access token for bundle service principal. Run `terraform apply` within 60 days to refresh before it expires."
  expiration_date = timeadd("${time_rotating.this.rfc3339}", "1440h")
}

resource "azurerm_key_vault_secret" "dbt_warehouse_id" {
  name            = "databricks-warehouse-dbt-id-${terraform.workspace}"
  value           = databricks_sql_endpoint.db_wh_dbt.id
  key_vault_id    = data.azurerm_key_vault.this.id
  content_type    = "Managed by Terraform. Id of the SQL Warehouse in Databricks that is used for dbt jobs."
  expiration_date = "2050-01-01T00:00:00Z"
}

resource "azurerm_key_vault_secret" "segment_sp_OAuthSecret" {
  name            = "databricks-sp-segment-oauthSecret-${terraform.workspace}"
  value           = databricks_service_principal_secret.segment_sp.secret
  key_vault_id    = data.azurerm_key_vault.this.id
  content_type    = "Managed by Terraform. OAuth secret of the Databricks Service Principal used in the Segment Connection."
  expiration_date = "2050-01-01T00:00:00Z"
}


##########################################
###               Grants               ###
##########################################

resource "databricks_grants" "catalog" {
  catalog = terraform.workspace

  grant {
    principal  = var.azure_client_id
    privileges = ["ALL_PRIVILEGES"]
  }

  grant {
    principal = databricks_service_principal.bundle_sp.application_id
    privileges = ["ALL_PRIVILEGES"]
  }

  grant {
    principal = databricks_service_principal.segment_sp.application_id
    privileges = ["ALL_PRIVILEGES"]
  }

  grant {
    principal = "data-scientists"
    privileges = terraform.workspace == "dev" ? ["ALL_PRIVILEGES"] : ["SELECT"]
  }

  grant {
    principal = "data-engineers"
    privileges = terraform.workspace == "dev" ? ["ALL_PRIVILEGES"] : ["SELECT"]
  }

  grant {
    principal = "data-analysts"
    privileges = terraform.workspace == "dev" ? ["ALL_PRIVILEGES"] : ["SELECT"]
  }
}

resource "databricks_grants" "catalog_segment" {
  catalog = databricks_catalog.segment.name

  grant {
    principal  = var.azure_client_id
    privileges = ["ALL_PRIVILEGES"]
  }

  grant {
    principal = databricks_service_principal.segment_sp.application_id
    privileges = ["ALL_PRIVILEGES"]
  }

  grant {
    principal = "data-engineers"
    privileges = ["ALL_PRIVILEGES"]
  }
}

resource "databricks_grants" "external_location" {
  for_each          = toset(var.schemas)
  external_location = databricks_external_location.this[each.key].id
  grant {
    principal  = var.azure_client_id
    privileges = ["ALL_PRIVILEGES"]
  }
  # Only add this all privileges if environment is dev
  grant {
    principal = "data-scientists"
    privileges = terraform.workspace == "dev" ? ["ALL_PRIVILEGES"] : ["READ_FILES"]
  }

  grant {
    principal = "data-engineers"
    privileges = terraform.workspace == "dev" ? ["ALL_PRIVILEGES"] : ["READ_FILES"]
  }

  grant {
    principal = "data-analysts"
    privileges = ["READ_FILES"]
  }

}


resource "databricks_grants" "external_location_segment" {
  external_location = databricks_external_location.segment.id
  grant {
    principal  = var.azure_client_id
    privileges = ["ALL_PRIVILEGES"]
  }
  # Only add this all privileges if environment is dev

  grant {
    principal = "data-engineers"
    privileges = terraform.workspace == "dev" ? ["ALL_PRIVILEGES"] : ["READ_FILES"]
  }

}


resource "databricks_grants" "schemas" {
  for_each          = toset(var.schemas)
  schema            = databricks_schema.this[each.key].id
  grant {
    principal  = var.azure_client_id
    privileges = ["ALL_PRIVILEGES"]
  }
  # Only add this all privileges if environment is dev
  grant {
    principal = "data-scientists"
    privileges = terraform.workspace == "dev" ? ["ALL_PRIVILEGES"] : ["USE_SCHEMA"]
  }

  grant {
    principal = "data-engineers"
    privileges = terraform.workspace == "dev" ? ["ALL_PRIVILEGES"] : ["USE_SCHEMA"]
  }

  grant {
    principal = "data-analysts"
    privileges = ["USE_SCHEMA"]
  }

}

resource "databricks_grants" "storage_credential" {
  storage_credential = databricks_storage_credential.this.id
  grant {
    principal  = var.azure_client_id
    privileges = ["ALL_PRIVILEGES"]
  }
}

##########################################
###               Outputs              ###
##########################################

output "databricks_workspace_url" {
  value = azurerm_databricks_workspace.this.workspace_url
}

output "databricks_id" {
  value = azurerm_databricks_workspace.this.id
}
