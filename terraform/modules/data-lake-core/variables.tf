variable "location" {
  type        = string
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
    "mlgold",
    "mlfeatures",
    "mloutputs",
    "mltesting",
    "mlfeaturetesting",
    "forecasting"
  ]
  description = "Names of containers within the Data Lake resource."
}

variable "ip_rules" {
  type        = list(string)
  description = "List of allowed IP addresses"
}

variable "databricks_access_connector_id" {
  type        = string
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
