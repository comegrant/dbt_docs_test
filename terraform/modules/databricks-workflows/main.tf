terraform {
  required_providers {
    databricks = {
      source = "databricks/databricks"
    }

  }
}

provider "databricks" {
  host                        = var.databricks_workspace_url
  azure_workspace_resource_id = var.databricks_id
}

data "databricks_node_type" "smallest" {
    local_disk = true    
}

data "databricks_spark_version" "latest_lts" {
    long_term_support = true
}