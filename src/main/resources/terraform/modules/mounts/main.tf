terraform {
  required_providers {
    databricks = {
      source  = "databrickslabs/databricks"
      version = "0.4.4"
    }
  }
}

resource "databricks_mount" "land" {
  name = "imp-internos-diot-land"
  uri  = "wasbs://imp-internos@cu1datosidentideclaracion${var.ambiente}staland.blob.core.windows.net"
  extra_configs = {
    "fs.azure.account.key.cu1dior${var.ambiente}staland.blob.core.windows.net" = "{{secrets/${var.scope_land}/${var.secret_land}}}"
  }
}

resource "databricks_mount" "bronce" {
  name = "imp-internos-diot-bronce"
  uri  = "wasbs://imp-internos@cu1datosidentideclaracion${var.ambiente}stabronce.blob.core.windows.net"
  extra_configs = {
    "fs.azure.account.key.cu1diot${var.ambiente}stabronce.blob.core.windows.net" = "{{secrets/${var.scope_bronce}/${var.secret_bronce}}}"
  }
}

resource "databricks_mount" "plata" {
  name = "imp-internos-diot-plata"
  uri  = "wasbs://imp-internos@cu1diot${var.ambiente}staplata.blob.core.windows.net"
  extra_configs = {
    "fs.azure.account.key.cu1diot${var.ambiente}staplata.blob.core.windows.net" = "{{secrets/${var.scope_plata}/${var.secret_plata}}}"
  }
}

resource "databricks_mount" "oro" {
  name = "imp-internos-diot-oro"
  uri  = "wasbs://imp-internos@cu1diot${var.ambiente}staoro.blob.core.windows.net"
  extra_configs = {
    "fs.azure.account.key.cu1diot${var.ambiente}staoro.blob.core.windows.net" = "{{secrets/${var.scope_oro}/${var.secret_oro}}}"
  }
}