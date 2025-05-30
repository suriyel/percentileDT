package com.example.percentile

import org.apache.spark.sql.SparkSession

/**
 * 快速测试程序，用于验证环境
 */
object QuickTest {

  def main(args: Array[String]): Unit = {
    val numRecords = if (args.length > 0) args(0).toLong else 1000000L

    println(s"快速测试 - 生成 $numRecords 条记录")

    val spark = SparkSession.builder()
      .appName("Quick Test")
      .master("local[2]") // 使用2个核心进行快速测试
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.driver.memory", "2g")
      .config("spark.executor.memory", "2g")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    try {
      // 生成测试数据
      val df = DataGenerator.generateData(spark, numRecords)

      println("数据基本信息:")
      df.printSchema()
      println(s"总行数: ${df.count()}")
      println(s"分组数: ${df.select("id").distinct().count()}")

      // 快速测试各种算法
      val calculator = new PercentileCalculator(spark)

      println("\n测试 approx_percentile...")
      val result1 = calculator.calculateApproxPercentile(df)
      println(s"approx_percentile 结果行数: ${result1.count()}")

      println("\n测试 percentile_approx...")
      val result2 = calculator.calculatePercentileApprox(df)
      println(s"percentile_approx 结果行数: ${result2.count()}")

      if (numRecords <= 100000) {
        println("\n测试精确计算...")
        val result3 = calculator.calculateExactPercentile(df)
        println(s"精确计算结果行数: ${result3.count()}")
      }

      println("\n快速测试完成！所有算法运行正常。")

    } catch {
      case e: Exception =>
        e.printStackTrace()
        println(s"测试失败: ${e.getMessage}")
    } finally {
      spark.stop()
    }
  }
}
