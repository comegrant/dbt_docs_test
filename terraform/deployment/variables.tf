variable "location" {
  type        = string
  description = "Location to deploy resources in."
}

variable "project_name" {
  type        = string
  description = "Name of the project of the core services."
}

variable "databricks_sku" {
  type        = string
  description = "Sku of Databricks."

}

variable "databricks_account_id" {
  type        = string
  description = "The account id of the Databricks Account containing all workspaces"
}

variable "medallion_layers" {
  type = list(string)
  default = [
    "bronze",
    "silver",
    "gold",
    "mltesting",
    "mlfeaturetesting",
    "snapshots",
    "mlgold",
    "mlfeatures",
    "mloutputs",
    "forecasting"
  ]
  description = "Names of containers and schemas within the Data Lake resource and Databricks catalogs, respectively."
}

variable "azure_client_id" {
  type        = string
  default     = "9d48429c-9bb6-4c22-ae63-26095e7aab6f"
  description = "Client id of service principal used for running the code."
}

variable "azure_client_secret" {
  type        = string
  description = "Client secret of service principal only used when running locally."
  default     = null
  sensitive   = true
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

variable "use_oidc" {
  type        = bool
  description = "Whether to use OIDC for authentication. This should be done when running in GitHub Actions, but not locally."
  default     = false
}