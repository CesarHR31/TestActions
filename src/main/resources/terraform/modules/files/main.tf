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

variable "codebase_root_path" {
  type = string
}

variable "jar_path" {
  type = string
}

resource "databricks_dbfs_file" "jar" {
  source = var.jar_path
  path   = "${var.dbfs_workspace_path}/sat-diot-parser_2.12-0.1.0.jar"
}

resource "databricks_dbfs_file" "diot_config_file" {
  source = "${var.codebase_root_path}/src/main/resources/databricks/code-config.json"
  path   = "${var.dbfs_workspace_path}/code-config.json"
}

resource "databricks_dbfs_file" "log4j2_config_file" {
  source = "${var.codebase_root_path}/src/main/resources/log4j2config.xml"
  path   = "${var.dbfs_workspace_path}/log4j2config.xml"
}

resource "databricks_dbfs_file" "gson_upgrade_jar" {
  source = "${var.codebase_root_path}/src/main/resources/gson-2.8.6.jar"
  path   = "/jars/gson-2.8.6.jar"
}

resource "databricks_dbfs_file" "gson_upgrade_bash" {
  source = "${var.codebase_root_path}/src/main/resources/gson-update.bash"
  path   = "${var.dbfs_workspace_path}/gson-update.bash"
}

output "jar_dbfs_path" {
  value = databricks_dbfs_file.jar.dbfs_path
}

output "gson_update_dbfs_path" {
  value = databricks_dbfs_file.gson_upgrade_bash.dbfs_path
}