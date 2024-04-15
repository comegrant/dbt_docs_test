variable "location" {
  type        = string
  description = "Location to deploy VNET in."
}

variable "resource_group_name" {
  type        = string
  description = "Resource group to deploy VNET in."
}

variable "virtual_network_name" {
  type        = string
  description = "Name of virtual network."
}

variable "subnet_name_prefix" {
  type        = string
  description = "Prefix of subnets which will be created."
}

variable "network_security_group_name" {
  type        = string
  description = "Name of network security group."
}