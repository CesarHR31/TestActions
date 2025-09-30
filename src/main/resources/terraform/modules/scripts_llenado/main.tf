terraform {
  required_providers {
    databricks = {
      source  = "databrickslabs/databricks"
      version = "0.4.4"
    }
  }
}

variable "dbfs_workspace_path" {
  type = string
}

variable "databricks_scripts_llenado" {
  type = string
}

resource "databricks_dbfs_file" "diot_llenado_tablas" {
  for_each = fileset(var.databricks_scripts_llenado, "*/*.sql")

  source = "${var.databricks_scripts_llenado}/${each.value}"
  path   = "${var.dbfs_workspace_path}/scripts-llenado/${each.value}"
}