name := "spark-percentile-benchmark"
version := "1.0"
scalaVersion := "2.12.17"

val sparkVersion = "3.4.0"

// 添加仓库配置
resolvers ++= Seq(
  "Maven Central" at "https://repo1.maven.org/maven2/",
  "Spark Packages Repo" at "https://repos.spark-packages.org/",
  "Typesafe Repository" at "https://repo.typesafe.com/typesafe/releases/"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion
)

// 排除不需要的传递依赖
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
