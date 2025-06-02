// ===================================================
// 修正后的 plugins.sbt - 兼容 SBT 1.8.2
// ===================================================

// Assembly插件 - 用于打包fat JAR (核心功能)
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "2.1.1")

// 代码覆盖率插件 (测试必需)
addSbtPlugin("org.scoverage" % "sbt-scoverage" % "2.0.7")

// 代码格式化插件 (可选但推荐)
addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.5.0")

// 依赖更新检查插件 (开发辅助)
addSbtPlugin("com.timushev.sbt" % "sbt-updates" % "0.6.4")

// ===================================================
// 以下插件暂时注释掉，避免版本兼容问题
// 可以根据需要逐个启用和测试
// ===================================================

// 依赖图生成插件 - 版本可能有兼容性问题
// addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.10.0-RC1")

// JUnit接口插件 - 不兼容当前SBT版本
// addSbtPlugin("com.github.sbt" % "sbt-junit-interface" % "0.13.3")

// 性能测试插件 - 可选功能
// addSbtPlugin("pl.project13.scala" % "sbt-jmh" % "0.4.4")

// Docker插件 - 非必需
// addSbtPlugin("com.typesafe.sbt" % "sbt-native-packager" % "1.9.9")

// 发布插件 - 非必需
// addSbtPlugin("com.github.sbt" % "sbt-release" % "1.1.0")

// 静态分析插件 - 非必需
// addSbtPlugin("com.github.sbt" % "sbt-findbugs" % "2.0.0")

// Git插件 - 非必需
// addSbtPlugin("com.github.sbt" % "sbt-git" % "2.0.1")