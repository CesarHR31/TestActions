terraform {
  required_providers {
    databricks = {
      source  = "databrickslabs/databricks"
      version = "0.4.4"
    }
  }
  backend "azurerm" {
    resource_group_name  = "CU1-DIOT-DEV-RGP-001"
    storage_account_name = "cu1diotdevstaland"
    container_name       = "terraform"
    key                  = "terraform_diot.state"
    access_key           = "yiQj1/ffihrDidiON/CPtUJ5Lrcd0NVbA1dyeR3d50kr1dJN0v7UKeI3sHIm3GhBJmY0Q4cTLn7S+ASt1ushsg=="
  }
}

provider "databricks" {
  config_file = "${terraform.workspace}.databrickscfg"
}

locals {
  codebase_root_path         = abspath("${path.module}/../../../..")
  jar_path                   = "${local.codebase_root_path}/target/scala-2.12/sat-diot-parser_2.12-0.1.0.jar"
  dbfs_workspace_path        = "/tmp/sat/diot"
  databricks_notebooks_path  = "${local.codebase_root_path}/src/main/resources/databricks/notebooks"  
  databricks_despliegue_path  = "${local.codebase_root_path}/src/main/resources/databricks/despliegue"  
  databricks_scripts_llenado = "${local.codebase_root_path}/src/main/resources/databricks/scripts-llenado"
}
/*
resource "null_resource" "sbt_clean_compile_package" {
  triggers = { always_run = "${timestamp()}" }
  provisioner "local-exec" {
    command     = "sbt package"
    working_dir = local.codebase_root_path
    interpreter = ["PowerShell", "-Command"]
  }
}*/

module "diot_databricks_files" {
  source = "./modules/files"

  dbfs_workspace_path = local.dbfs_workspace_path
  codebase_root_path  = local.codebase_root_path
  jar_path            = local.jar_path
  
  //depends_on = [null_resource.sbt_clean_compile_package]
}

module "montajes_databricks" {
  source = "./modules/mounts"

  scope_bronce  = var.scope_bronce
  scope_land    = var.scope_land
  scope_plata   = var.scope_plata
  scope_oro     = var.scope_oro
  secret_bronce = var.secret_bronce
  secret_land   = var.secret_land
  secret_plata  = var.secret_plata
  secret_oro    = var.secret_oro
  ambiente      = var.ambiente
}

module "scripts_llenado" {
  source = "./modules/scripts_llenado"

  databricks_scripts_llenado = local.databricks_scripts_llenado
  dbfs_workspace_path        = local.dbfs_workspace_path
}

module "notebooks_ejecucion" {
  source = "./modules/notebooks_ejecucion"

  databricks_notebooks_path = local.databricks_notebooks_path  
}

module "notebooks_despliegue" {
  source = "./modules/notebooks_despliegue"

  databricks_despliegue_path = local.databricks_despliegue_path  
}