name := "spark-percentile-benchmark"
version := "1.0"
scalaVersion := "2.12.17"

val sparkVersion = "3.4.0"

// 仓库配置
resolvers ++= Seq(
  "Maven Central" at "https://repo1.maven.org/maven2/"
)

// 依赖配置
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.scalatest" %% "scalatest" % "3.2.15" % Test
)

// 排除冲突依赖
libraryDependencies := libraryDependencies.value.map(_.exclude("org.slf4j", "slf4j-log4j12"))

// 编译选项
scalacOptions ++= Seq(
  "-deprecation",
  "-feature",
  "-unchecked"
)

// JVM选项
javaOptions ++= Seq(
  "-Xmx8g",
  "-XX:+UseG1GC"
)

// 测试配置
Test / parallelExecution := false
Test / fork := true
Test / javaOptions ++= Seq(
  "-Xmx4g",
  "-XX:+UseG1GC",
  "-Dspark.ui.enabled=false",
  "-Dspark.master=local[2]"
)