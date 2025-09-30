
ThisBuild / version := "0.1.0"

ThisBuild / scalaVersion := "2.12.15"

lazy val root = (project in file("."))
  .settings(
    name := "sat-diot-parser"
  )

val sparkVersion = "3.4.1"

val targetJvm = settingKey[String]("Target JVM version")
Global / targetJvm := "1.8"

dependencyOverrides := Seq(
  "org.antlr" % "antlr4-runtime" % "4.9.3"
)

javaOptions ++= Seq(
  "--add-exports", "java.base/sun.nio.ch=ALL-UNNAMED"
)

libraryDependencies += "com.databricks" % "dbutils-api_2.12" % "0.0.6"
libraryDependencies += "com.github.docker-java" % "docker-java" % "3.3.0"
libraryDependencies += "com.github.docker-java" % "docker-java-transport-httpclient5" % "3.3.0"
libraryDependencies += "com.google.code.gson" % "gson" % "2.8.6"
libraryDependencies += "com.microsoft.azure" % "applicationinsights-core" % "2.6.4"
// https://mvnrepository.com/artifact/com.azure/azure-data-tables
//libraryDependencies += "com.azure" % "azure-data-tables" % "12.4.4"
libraryDependencies += "com.github.jsurfer" % "jsurfer-gson" % "1.6.5"
libraryDependencies += "com.azure" % "azure-storage-blob" % "12.24.1"
libraryDependencies += "com.microsoft.azure" % "azure-storage" % "8.6.6"
libraryDependencies += "com.microsoft.sqlserver" % "mssql-jdbc" % "9.2.1.jre8"
libraryDependencies += "com.typesafe.slick" %% "slick" % "3.5.1"
libraryDependencies += "io.delta" %% "delta-core" % "2.4.0"
libraryDependencies += "org.apache.poi" % "poi" % "5.3.0"
libraryDependencies += "org.apache.poi" % "poi-ooxml" % "5.3.0"
libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion % "provided"
libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion % "test"
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % "test"
libraryDependencies += "org.postgresql" % "postgresql" % "42.7.3"
libraryDependencies += "org.scalatest" %% "scalatest-flatspec" % "3.2.19" % "test"
libraryDependencies += "org.scalatest" %% "scalatest-funsuite" % "3.2.19" % "test"
libraryDependencies += "org.apache.commons" % "commons-dbcp2" % "2.9.0"