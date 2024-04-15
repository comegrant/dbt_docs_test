variable "location" {
  type        = string
  #default     = "North Europe"
  description = "Azure region to locate resource."
}

variable "resource_group_name" {
  type        = string
  description = "Resource group to contain resource."
}

variable "data_lake_name" {
  type        = string
  description = "Name of Data Lake."
}

variable "data_lake_containers" {
  type = list(string)
  default = [
    "bronze",
    "silver",
    "gold",
  ]
  description = "Names of containers within the Data Lake resource."
}

variable "data_lake_tier" {
  type        = string
  default     = "Standard"
  description = "Tier of Data Lake."
}

variable "data_lake_replication_type" {
  type        = string
  default     = "LRS"
  description = "Replication type of Data Lake."
}

variable "databricks_service_principal_name" {
  type        = string
  description = "Name of the Databricks service principal."
}

variable "ip_rules" {
  type        = list(string)
  description = "List of allowed IP addresses"
}

variable "databricks_access_connector_id" {
  type = string
  description = "Principal Id of managed identity of the Access connector of Azure Databricks."
}


variable "private_endpoint_name" {
  type        = string
  description = "Name of the private endpoint resource."
}

variable "network_interface_name" {
  type        = string
  description = "Name of the network interfaco resource which will be created along with private endpoint."
}

variable "virtual_network_resource_group_name" {
  type        = string
  description = "Resource group which contains the vnet which the private endpoint is set up to."
}

variable "virtual_network_id" {
  type        = string
  description = "Resource ID of the virtual network which the private endpoint is set up to."
}

variable "subnet_id" {
  type        = string
  description = "Resource ID of the subnet which the private endpoint is set up to."
}