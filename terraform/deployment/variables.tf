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

variable "databricks_service_principal_name" {
  type        = string
  description = "Name of service principal that will act as Databricks' identity."
}

variable "medallion_layers" {
  type = list(string)
  default = [
    "bronze",
    "silver",
    "gold",
    "mltesting"
  ]
  description = "Names of containers and schemas within the Data Lake resource and Databricks catalogs, respectively."
}

variable "databricks_spark_version" {
  type        = string
  description = "Spark runtime on clusters."
}