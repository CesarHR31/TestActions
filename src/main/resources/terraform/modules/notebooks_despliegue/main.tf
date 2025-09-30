terraform {
  required_providers {
    databricks = {
      source  = "databrickslabs/databricks"
      version = "0.4.4"
    }
  }
}

variable "databricks_despliegue_path" {
  type = string
}

resource "databricks_notebook" "_Mounts" {
  path           = "/Shared/DWH-DIOT/Despliegue/databricks/01_Mounts"
  language       = "SCALA"
  content_base64 = filebase64("${var.databricks_despliegue_path}/databricks/01_Mounts.scala")
}

resource "databricks_notebook" "_Crear_Base_Desdoble" {
  path           = "/Shared/DWH-DIOT/Despliegue/databricks/02_Crear_Base_Desdoble"
  language       = "SQL"
  content_base64 = filebase64("${var.databricks_despliegue_path}/databricks/02_Crear_Base_Desdoble.sql")
}

resource "databricks_notebook" "_03_Rollback" {
  path           = "/Shared/DWH-DIOT/Despliegue/databricks/03_Rollback"
  language       = "SCALA"
  content_base64 = filebase64("${var.databricks_despliegue_path}/databricks/03_Rollback.scala")
}

resource "databricks_notebook" "_Crear_tablas_control" {
  path           = "/Shared/DWH-DIOT/Despliegue/single/01_Crear_tablas_control"
  language       = "SCALA"
  content_base64 = filebase64("${var.databricks_despliegue_path}/single/01_Crear_tablas_control.sql")
}

resource "databricks_notebook" "_02_Rollback" {
  path           = "/Shared/DWH-DIOT/Despliegue/single/02_Rollback"
  language       = "SCALA"
  content_base64 = filebase64("${var.databricks_despliegue_path}/single/02_Rollback.sql")
}