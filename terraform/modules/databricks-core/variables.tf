variable "location" {
  type        = string
  description = "Region to deploy resource in."
}

variable "resource_group_workspace_name" {
  type        = string
  description = "Resource group to contain Databricks workspace resource."
}

variable "resource_group_managed_name" {
  type        = string
  description = "Name of the resource group managed by the Databricks instance."
}

variable "storage_account_managed_name" {
  type        = string
  description = "Name of the storage account managed by the Databricks instance."
}

variable "databricks_service_principal_name" {
  type        = string
  description = "Name of the service principal which will act as the Databricks identity."
}

variable "data_lake_name" {
  type        = string
  description = "Name of the Data Lake which will act as the Databricks' Delta Lake."
}

variable "databricks_workspace_name" {
  type        = string
  description = "Name of the Databricks workspace."
}

variable "azure_databricks_access_connector_name" {
  type        = string
  description = "Name of the Access Connector for Azure Databricks"
}

variable "databricks_sku" {
  type        = string
  description = "Tier of the Databricks instance."
  validation {
    condition     = lower(var.databricks_sku) == "standard" || lower(var.databricks_sku) == "premium"
    error_message = "The SKU of the Databricks instance must be either standard or premium."
  }
}

variable "key_vault_common_name" {
  type        = string
  description = "Name of key vault used for common secrets"
}


variable "resource_group_common_name" {
  type        = string
  description = "Name of resource used for common resources"
}

variable "databricks_catalog_name" {
  type        = string
  description = "Name of the catalog connected to the workspace"
}

variable "schemas" {
  type = list(string)
  default = [
    "bronze",
    "silver",
    "gold",
  ]
  description = "Names of schemas within the Databricks catalogs."
}

variable "resource_group_vnet_name" {
  type        = string
  description = "Resource group which contains the virtual network that Databricks will be injected in."

}

variable "virtual_network_id" {
  type        = string
  description = "Resource ID of virtual network which Databricks will be injected in."
}

variable "subnet_public_name" {
  type        = string
  description = "Name of the subnet which will act as the host/public subnet."
}

variable "subnet_private_name" {
  type        = string
  description = "Name of the subnet which will act as the container/private subnet."
}

variable "nsg_association_public_id" {
  type        = string
  description = "?"
}

variable "nsg_association_private_id" {
  type        = string
  description = "?"
}

variable "databricks_node_type" {
  type        = string
  default     = "Standard_F4"
  description = "Databricks cluster VM type."
}

variable "databricks_pool_min_idle" {
  type        = number
  default     = 0
  description = "Lower bound for idle nodes in instance pool."
}

variable "databricks_pool_max_capacity" {
  type        = number
  default     = 10
  description = "Upper bound for idle nodes in instance pool."
}

variable "databricks_pool_idle_termination" {
  type        = number
  default     = 10
  description = "Minutes before idle nodes in instance pool are released."
}

variable "databricks_cluster_idle_termination" {
  type        = number
  default     = 40
  description = "Shut down time after inactivity on all purpose cluster."
}

variable "databricks_spark_version" {
  type        = string
  description = "Spark runtime."
}

variable "databricks_cluster_min_workers" {
  type        = number
  default     = 1
  description = "Lower bound for worker nodes on all purpose cluster."
}

variable "databricks_cluster_max_workers" {
  type        = number
  default     = 4
  description = "Upper bound for worker nodes on all purpose cluster."
}

variable "databricks_serverless_sql_cluster_size" {
  type        = string
  default     = "2X-Small"
  description = "Size of sql warehouse"
}

variable "databricks_serverless_sql_min_num_clusters" {
  type        = number
  default     = 1
  description = "Minimum number of clusters available when a SQL warehouse is running"
}

variable "databricks_serverless_sql_max_num_clusters" {
  type        = number
  default     = 2
  description = "Maximum number of clusters available when a SQL warehouse is running"
} 

variable "databricks_serverless_sql_idle_termination" {
  type        = number
  default     = 10
  description = "Shut down time after inactivity on serverless sql warehouse."
}