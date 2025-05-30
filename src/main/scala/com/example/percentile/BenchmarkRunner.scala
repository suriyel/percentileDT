package com.example.percentile

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import java.io.{File, PrintWriter}

/**
 * 性能测试运行器
 */
class BenchmarkRunner(spark: SparkSession) {

  case class BenchmarkResult(
                              method: String,
                              executionTime: Long,
                              resultCount: Long,
                              avgGroupSize: Double
                            )

  def runBenchmark(df: DataFrame, outputPath: String): Unit = {
    val calculator = new PercentileCalculator(spark)
    val results = scala.collection.mutable.ArrayBuffer[BenchmarkResult]()

    // 预热JVM
    println("JVM预热中...")
    val warmupDf = df.sample(0.01)
    calculator.calculateApproxPercentile(warmupDf).count()

    println("开始性能测试...")

    // 获取基础统计信息
    val totalRecords = df.count()
    val groupCount = df.select("id").distinct().count()
    val avgGroupSize = totalRecords.toDouble / groupCount

    println(s"总记录数: $totalRecords")
    println(s"分组数: $groupCount")
    println(s"平均每组记录数: ${avgGroupSize.formatted("%.2f")}")

    // 测试1: approx_percentile
    try {
      val startTime = System.currentTimeMillis()
      val result1 = calculator.calculateApproxPercentile(df)
      result1.cache()
      val count1 = result1.count()
      val endTime = System.currentTimeMillis()

      results += BenchmarkResult("approx_percentile", endTime - startTime, count1, avgGroupSize)
      result1.unpersist()
    } catch {
      case e: Exception =>
        println(s"approx_percentile 测试失败: ${e.getMessage}")
        results += BenchmarkResult("approx_percentile", -1, -1, avgGroupSize)
    }

    // 测试2: percentile_approx
    try {
      val startTime = System.currentTimeMillis()
      val result2 = calculator.calculatePercentileApprox(df)
      result2.cache()
      val count2 = result2.count()
      val endTime = System.currentTimeMillis()

      results += BenchmarkResult("percentile_approx", endTime - startTime, count2, avgGroupSize)
      result2.unpersist()
    } catch {
      case e: Exception =>
        println(s"percentile_approx 测试失败: ${e.getMessage}")
        results += BenchmarkResult("percentile_approx", -1, -1, avgGroupSize)
    }

    // 测试3: 精确计算（仅在数据量不太大时）
    if (totalRecords <= 10000000) { // 1000万以下才做精确计算
      try {
        val startTime = System.currentTimeMillis()
        val result3 = calculator.calculateExactPercentile(df)
        result3.cache()
        val count3 = result3.count()
        val endTime = System.currentTimeMillis()

        results += BenchmarkResult("exact_percentile", endTime - startTime, count3, avgGroupSize)
        result3.unpersist()
      } catch {
        case e: Exception =>
          println(s"exact_percentile 测试失败: ${e.getMessage}")
          results += BenchmarkResult("exact_percentile", -1, -1, avgGroupSize)
      }
    }

    // 测试4: collect计算（仅在很小的数据集上）
    if (totalRecords <= 1000000) { // 100万以下才做collect计算
      try {
        val startTime = System.currentTimeMillis()
        val result4 = calculator.calculateCollectPercentile(df)
        result4.cache()
        val count4 = result4.count()
        val endTime = System.currentTimeMillis()

        results += BenchmarkResult("collect_percentile", endTime - startTime, count4, avgGroupSize)
        result4.unpersist()
      } catch {
        case e: Exception =>
          println(s"collect_percentile 测试失败: ${e.getMessage}")
          results += BenchmarkResult("collect_percentile", -1, -1, avgGroupSize)
      }
    }

    // 输出结果
    printResults(results.toSeq)
    saveResults(results.toSeq, outputPath)
  }

  def runAccuracyTest(df: DataFrame): Unit = {
    println("开始精确性测试...")

    // 使用小数据集进行精确性对比
    val smallDf = df.sample(0.001).cache() // 采样0.1%的数据
    val calculator = new PercentileCalculator(spark)

    try {
      val approxResult = calculator.calculateApproxPercentile(smallDf)
      val exactResult = calculator.calculateExactPercentile(smallDf)

      // 计算误差
      val joinedResult = approxResult.join(exactResult, Seq("id"))

      // 这里可以添加具体的误差计算逻辑
      println("精确性测试完成，详细对比需要进一步实现")

    } catch {
      case e: Exception => println(s"精确性测试失败: ${e.getMessage}")
    } finally {
      smallDf.unpersist()
    }
  }

  private def printResults(results: Seq[BenchmarkResult]): Unit = {
    println("\n" + "="*80)
    println("性能测试结果")
    println("="*80)
    printf("%-20s %-15s %-15s %-15s\n", "方法", "执行时间(ms)", "结果行数", "平均组大小")
    println("-"*80)

    for (result <- results) {
      val timeStr = if (result.executionTime == -1) "失败" else result.executionTime.toString
      val countStr = if (result.resultCount == -1) "失败" else result.resultCount.toString
      printf("%-20s %-15s %-15s %-15.2f\n",
        result.method, timeStr, countStr, result.avgGroupSize)
    }
    println("="*80)
  }

  private def saveResults(results: Seq[BenchmarkResult], outputPath: String): Unit = {
    val file = new File(outputPath)
    file.getParentFile.mkdirs()

    val writer = new PrintWriter(file)
    try {
      writer.println("method,execution_time_ms,result_count,avg_group_size")
      for (result <- results) {
        writer.println(s"${result.method},${result.executionTime},${result.resultCount},${result.avgGroupSize}")
      }
    } finally {
      writer.close()
    }

    println(s"测试结果已保存到: $outputPath")
  }
}