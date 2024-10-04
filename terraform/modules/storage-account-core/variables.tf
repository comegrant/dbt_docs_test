variable "resource_group_name" {
  type        = string
  description = "Resource group to contain resource."
}

variable "location" {
  type        = string
  description = "Azure region to locate resource."
}

variable "storage_account_name" {
  type        = string
  description = "Name of the storage account."
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

variable "storage_container_names" {
  description = "List of names for the storage containers to be created"
  type        = list(string)
}

variable "common_key_vault_id" {
  description = "The ID of the common key vault where the storage account key will be stored"
  type        = string
}
