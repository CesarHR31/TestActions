terraform {
  required_providers {
    databricks = {
      source  = "databrickslabs/databricks"
      version = "0.4.4"
    }
  }
}

variable "databricks_notebooks_path" {
  type = string
}

resource "databricks_notebook" "_Land" {
  path           = "/Shared/DWH-DIOT/1_Land"
  language       = "SCALA"
  content_base64 = filebase64("${var.databricks_notebooks_path}/1_Land.scala")
}

resource "databricks_notebook" "_Bronce" {
  path           = "/Shared/DWH-DIOT/2_Bronce"
  language       = "SCALA"
  content_base64 = filebase64("${var.databricks_notebooks_path}/2_Bronce.scala")
}

resource "databricks_notebook" "_Plata" {
  path           = "/Shared/DWH-DIOT/3_Plata"
  language       = "SCALA"
  content_base64 = filebase64("${var.databricks_notebooks_path}/3_Plata.scala")
}

resource "databricks_notebook" "_ConciliacionPlata" {
  path           = "/Shared/DWH-DIOT/4_ConciliacionPlata"
  language       = "SCALA"
  content_base64 = filebase64("${var.databricks_notebooks_path}/4_ConciliacionPlata.scala")
}

resource "databricks_notebook" "_Oro" {
  path           = "/Shared/DWH-DIOT/5_Oro"
  language       = "SCALA"
  content_base64 = filebase64("${var.databricks_notebooks_path}/5_Oro.scala")
}

resource "databricks_notebook" "_ConciliacionOro" {
  path           = "/Shared/DWH-DIOT/6_ConciliacionOro"
  language       = "SCALA"
  content_base64 = filebase64("${var.databricks_notebooks_path}/6_ConciliacionOro.scala")
}

resource "databricks_notebook" "_CsvDeltas" {
  path           = "/Shared/DWH-DIOT/7_CsvDeltas"
  language       = "SCALA"
  content_base64 = filebase64("${var.databricks_notebooks_path}/7_CsvDeltas.scala")
}

resource "databricks_notebook" "_Ejecutor_Desdoble" {
  path           = "/Shared/DWH-DIOT/Ejecutor_Desdoble"
  language       = "SCALA"
  content_base64 = filebase64("${var.databricks_notebooks_path}/Ejecutor_Desdoble.scala")
}