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

variable "data_lake_name" {
  type        = string
  description = "Name of the Data Lake which will act as the Databricks' Delta Lake."
}

variable "databricks_workspace_name" {
  type        = string
  description = "Name of the Databricks workspace."
}

variable "databricks_repo_path" {
  type        = string
  description = "Path in databricks to repo with code that will be used in jobs etc."
}

variable "azure_databricks_access_connector_name" {
  type        = string
  description = "Name of the Access Connector for Azure Databricks"
}

variable "access_connector_id" {
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
    "mltesting",
  ]
  description = "Names of schemas within the Databricks catalogs."
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

variable "azure_client_id" {
  type        = string
  description = "Client id of service principal used in Github Actions for running the code."
}

variable "azure_subscription_id" {
  type        = string
  default     = "5a07602a-a1a5-43ee-9770-2cf18d1fdaf1"
  description = "Subscription id of the subscription where the service principal have permission and resources should be deployed."
}

variable "azure_tenant_id" {
  type        = string
  default     = "f02c0daa-f4a6-41df-9fbb-df3be1b2b577"
  description = "Tenant Id of the tenant where the service princial exist and resources will be deployed."
}