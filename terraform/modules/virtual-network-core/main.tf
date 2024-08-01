terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.29.0"
    }
  }
}

variable "azure_client_id" {}
variable "azure_subscription_id" {}
variable "azure_tenant_id" {}

provider "azurerm" {
  features {}
  use_oidc        = true
  client_id       = var.azure_client_id
  subscription_id = var.azure_subscription_id
  tenant_id       = var.azure_tenant_id
}

resource "azurerm_virtual_network" "this" {
  address_space       = ["172.16.0.0/24"]
  location            = var.location
  name                = var.virtual_network_name
  resource_group_name = var.resource_group_name
}

resource "azurerm_network_security_group" "this" {
  name                = var.network_security_group_name
  location            = azurerm_virtual_network.this.location
  resource_group_name = azurerm_virtual_network.this.resource_group_name

  security_rule {
    name                       = "Databricks-workspaces_UseOnly_databricks-control-plane-to-worker-proxy"
    description                = "Required for Databricks control plane communication with worker nodes."
    protocol                   = "Tcp"
    source_port_range          = "*"
    destination_port_range     = "5557"
    source_address_prefix      = "AzureDatabricks"
    destination_address_prefix = "VirtualNetwork"
    access                     = "Allow"
    priority                   = 101
    direction                  = "Inbound"
  }

  security_rule {
    name                       = "Databricks-workspaces_UseOnly_databricks-control-plane-to-worker-ssh"
    description                = "Required for Databricks control plane management of worker nodes."
    protocol                   = "Tcp"
    source_port_range          = "*"
    destination_port_range     = "22"
    source_address_prefix      = "AzureDatabricks"
    destination_address_prefix = "VirtualNetwork"
    access                     = "Allow"
    priority                   = 100
    direction                  = "Inbound"
  }

  security_rule {
    name                       = "Databricks-workspaces_UseOnly_databricks-worker-to-databricks-webapp"
    description                = "Required for workers communication with Databricks Webapp."
    protocol                   = "Tcp"
    source_port_range          = "*"
    destination_port_ranges    = ["443","3306","8443-8451"]
    source_address_prefix      = "VirtualNetwork"
    destination_address_prefix = "AzureDatabricks"
    access                     = "Allow"
    priority                   = 100
    direction                  = "Outbound"
  }

  security_rule {
    name                       = "Databricks-workspaces_UseOnly_databricks-worker-to-eventhub"
    description                = "Required for worker communication with Azure Eventhub services."
    protocol                   = "Tcp"
    source_port_range          = "*"
    destination_port_range     = "9093"
    source_address_prefix      = "VirtualNetwork"
    destination_address_prefix = "EventHub"
    access                     = "Allow"
    priority                   = 104
    direction                  = "Outbound"
  }

  security_rule {
    name                       = "Databricks-workspaces_UseOnly_databricks-worker-to-sql"
    description                = "Required for workers communication with Azure SQL services."
    protocol                   = "Tcp"
    source_port_range          = "*"
    destination_port_range     = "3306"
    source_address_prefix      = "VirtualNetwork"
    destination_address_prefix = "Sql"
    access                     = "Allow"
    priority                   = 101
    direction                  = "Outbound"
  }

  security_rule {
    name                       = "Databricks-workspaces_UseOnly_databricks-worker-to-storage"
    description                = "Required for workers communication with Azure Storage services."
    protocol                   = "Tcp"
    source_port_range          = "*"
    destination_port_range     = "443"
    source_address_prefix      = "VirtualNetwork"
    destination_address_prefix = "Storage"
    access                     = "Allow"
    priority                   = 102
    direction                  = "Outbound"
  }

  security_rule {
    name                       = "Databricks-workspaces_UseOnly_databricks-worker-to-worker-inbound"
    description                = "Required for worker nodes communication within a cluster."
    protocol                   = "*"
    source_port_range          = "*"
    destination_port_range     = "*"
    source_address_prefix      = "VirtualNetwork"
    destination_address_prefix = "VirtualNetwork"
    access                     = "Allow"
    priority                   = 102
    direction                  = "Inbound"
  }

  security_rule {
    name                       = "Databricks-workspaces_UseOnly_databricks-worker-to-worker-outbound"
    description                = "Required for worker nodes communication within a cluster."
    protocol                   = "*"
    source_port_range          = "*"
    destination_port_range     = "*"
    source_address_prefix      = "VirtualNetwork"
    destination_address_prefix = "VirtualNetwork"
    access                     = "Allow"
    priority                   = 103
    direction                  = "Outbound"
  }

}

resource "azurerm_subnet" "public" {
  address_prefixes     = ["172.16.0.0/26"]
  name                 = "${var.subnet_name_prefix}-public"
  resource_group_name  = azurerm_virtual_network.this.resource_group_name
  virtual_network_name = azurerm_virtual_network.this.name

  delegation {
    name = "delegation"

    service_delegation {
      actions = [
        "Microsoft.Network/virtualNetworks/subnets/join/action",
        "Microsoft.Network/virtualNetworks/subnets/prepareNetworkPolicies/action",
        "Microsoft.Network/virtualNetworks/subnets/unprepareNetworkPolicies/action",
      ]
      name = "Microsoft.Databricks/workspaces"
    }
  }
}

resource "azurerm_subnet" "private" {
  address_prefixes     = ["172.16.0.64/26"]
  name                 = "${var.subnet_name_prefix}-private"
  resource_group_name  = azurerm_virtual_network.this.resource_group_name
  virtual_network_name = azurerm_virtual_network.this.name

  delegation {
    name = "delegation"

    service_delegation {
      actions = [
        "Microsoft.Network/virtualNetworks/subnets/join/action",
        "Microsoft.Network/virtualNetworks/subnets/prepareNetworkPolicies/action",
        "Microsoft.Network/virtualNetworks/subnets/unprepareNetworkPolicies/action",
      ]
      name = "Microsoft.Databricks/workspaces"
    }
  }
}

resource "azurerm_subnet" "div1" {
  address_prefixes     = ["172.16.0.128/27"]
  name                 = "${var.subnet_name_prefix}-1"
  resource_group_name  = azurerm_virtual_network.this.resource_group_name
  virtual_network_name = azurerm_virtual_network.this.name
}


resource "azurerm_subnet" "div2" {
  address_prefixes     = ["172.16.0.160/27"]
  name                 = "${var.subnet_name_prefix}-2"
  resource_group_name  = azurerm_virtual_network.this.resource_group_name
  virtual_network_name = azurerm_virtual_network.this.name
}

resource "azurerm_subnet" "div3" {
  address_prefixes     = ["172.16.0.192/27"]
  name                 = "${var.subnet_name_prefix}-3"
  resource_group_name  = azurerm_virtual_network.this.resource_group_name
  virtual_network_name = azurerm_virtual_network.this.name
}

resource "azurerm_subnet" "div4" {
  address_prefixes     = ["172.16.0.224/27"]
  name                 = "${var.subnet_name_prefix}-4"
  resource_group_name  = azurerm_virtual_network.this.resource_group_name
  virtual_network_name = azurerm_virtual_network.this.name
}

resource "azurerm_subnet_network_security_group_association" "public" {
  subnet_id                 = azurerm_subnet.public.id
  network_security_group_id = azurerm_network_security_group.this.id
}

resource "azurerm_subnet_network_security_group_association" "private" {
  subnet_id                 = azurerm_subnet.private.id
  network_security_group_id = azurerm_network_security_group.this.id
}

output "name" {
  value = azurerm_virtual_network.this.name
}

output "id" {
  value = azurerm_virtual_network.this.id
}

output "resource_group_name" {
  value = azurerm_virtual_network.this.resource_group_name
}

output "subnet_public_name" {
  value = azurerm_subnet.public.name
}

output "subnet_private_name" {
  value = azurerm_subnet.private.name
}

output "subnet_endpoints_id" {
  value = azurerm_subnet.div1.id
}

output "location" {
  value = azurerm_virtual_network.this.location
}

output "nsg_association_public_id" {
  value = azurerm_subnet_network_security_group_association.public.id
}

output "nsg_association_private_id" {
  value = azurerm_subnet_network_security_group_association.private.id
}