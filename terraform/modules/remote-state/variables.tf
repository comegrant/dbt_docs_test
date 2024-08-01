variable "location" {
  type        = string
  default     = "North Europe"
  description = "Region to deploy resource in."
}

variable "resource_group_name" {
  type        = string
  default     = "rg-chefdp-tfstate"
  description = "Name of resource group."
}

variable "storage_account_name" {
  type        = string
  default     = "stchefdptfstate"
  description = "Name of storage account."
}


variable "key_vault_name" {
  type        = string
  default     = "kv-chefdp-tfstate"
  description = "Name of key vault"
}