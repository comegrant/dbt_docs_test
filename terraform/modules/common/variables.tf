variable "location" {
  type        = string
  description = "Azure region to locate resource."
}

variable "environment" {
  type        = string
  description = "Environment. Will be set to 'common' for the common resources."
}

variable "project_name" {
  type        = string
  description = "Name of the project of the core services."
}


variable "databricks_service_principal_name" {
  type        = string
  description = "Name of the service principal which will act as the Databricks identity."
}