package com.example.percentile

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.io.{File, PrintWriter}

/**
 * 性能测试运行器
 */
class BenchmarkRunner(spark: SparkSession) {
  import spark.implicits._

  case class BenchmarkResult(
                              method: String,
                              executionTime: Long,
                              resultCount: Long,
                              avgGroupSize: Double
                            )

  case class AccuracyResult(
                             method: String,
                             column: String,
                             percentile: Double,
                             avgAbsError: Double,
                             avgRelError: Double,
                             maxAbsError: Double,
                             maxRelError: Double,
                             errorStdDev: Double,
                             sampleCount: Long
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

    // 测试2: percentile
    try {
      val startTime = System.currentTimeMillis()
      val result2 = calculator.calculatePercentile(df)
      result2.cache()
      val count2 = result2.count()
      val endTime = System.currentTimeMillis()

      results += BenchmarkResult("percentile", endTime - startTime, count2, avgGroupSize)
      result2.unpersist()
    } catch {
      case e: Exception =>
        println(s"percentile 测试失败: ${e.getMessage}")
        results += BenchmarkResult("percentile", -1, -1, avgGroupSize)
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
    println("\n" + "="*80)
    println("开始精确性测试...")
    println("="*80)

    // 使用适当大小的数据集进行精确性对比
    val testSizes = Array(0.001, 0.01) // 0.1%和1%的数据

    for (sampleRate <- testSizes) {
      println(s"\n测试数据量: ${(sampleRate * 100).formatted("%.1f")}%")
      val testDf = df.sample(sampleRate).cache()

      try {
        val totalRecords = testDf.count()
        println(s"样本记录数: $totalRecords")

        if (totalRecords > 0 && totalRecords <= 5000000) { // 最多500万条记录
          runDetailedAccuracyTest(testDf, sampleRate)
        } else {
          println("样本数据量不适合精确性测试")
        }
      } catch {
        case e: Exception =>
          println(s"精确性测试失败: ${e.getMessage}")
      } finally {
        testDf.unpersist()
      }
    }
  }

  private def runDetailedAccuracyTest(df: DataFrame, sampleRate: Double): Unit = {
    val calculator = new PercentileCalculator(spark)
    val results = scala.collection.mutable.ArrayBuffer[AccuracyResult]()

    try {
      println("计算精确分位点...")
      val exactResult = calculator.calculateExactPercentile(df).cache()

      println("计算近似分位点 (approx_percentile)...")
      val approxResult1 = calculator.calculateApproxPercentile(df).cache()

      println("计算近似分位点 (percentile)...")
      val approxResult2 = calculator.calculatePercentile(df).cache()

      // 对比approx_percentile与精确结果
      val comparison1 = compareResults(exactResult, approxResult1, "approx_percentile")
      results ++= comparison1

      // 对比percentile与精确结果
      val comparison2 = compareResults(exactResult, approxResult2, "percentile")
      results ++= comparison2

      // 保存精确性测试结果
      val accuracyPath = s"results/accuracy_results_${(sampleRate * 100).toInt}pct.csv"
      saveAccuracyResults(results.toSeq, accuracyPath)

      // 打印精确性分析
      printAccuracyAnalysis(results.toSeq)

      // 清理缓存
      exactResult.unpersist()
      approxResult1.unpersist()
      approxResult2.unpersist()

    } catch {
      case e: Exception =>
        println(s"详细精确性测试失败: ${e.getMessage}")
        e.printStackTrace()
    }
  }

  private def compareResults(exactDf: DataFrame, approxDf: DataFrame, method: String): Seq[AccuracyResult] = {
    val results = scala.collection.mutable.ArrayBuffer[AccuracyResult]()
    val valueColumns = Array("value1", "value2", "value3", "value4", "value5")
    val percentiles = Array(0.25, 0.5, 0.75, 0.9, 0.95, 0.99)

    // 连接精确结果和近似结果
    val joinedDf = exactDf.join(approxDf, Seq("id"), "inner")

    for (valueCol <- valueColumns; p <- percentiles) {
      val exactCol = s"${valueCol}_p${(p * 100).toInt}_exact"
      val approxCol = method match {
        case "approx_percentile" => s"${valueCol}_p${(p * 100).toInt}_approx"
        case "percentile" => s"${valueCol}_p${(p * 100).toInt}_approx2"
        case _ => s"${valueCol}_p${(p * 100).toInt}_approx"
      }

      try {
        // 计算误差统计
        val errorDf = joinedDf
          .filter(col(exactCol).isNotNull && col(approxCol).isNotNull)
          .withColumn("abs_error", abs(col(approxCol) - col(exactCol)))
          .withColumn("rel_error",
            when(col(exactCol) =!= 0.0,
              abs((col(approxCol) - col(exactCol)) / col(exactCol))
            ).otherwise(0.0)
          )
          .cache()

        val errorStats = errorDf.agg(
          avg("abs_error").as("avg_abs_error"),
          avg("rel_error").as("avg_rel_error"),
          max("abs_error").as("max_abs_error"),
          max("rel_error").as("max_rel_error"),
          stddev("abs_error").as("error_stddev"),
          count("*").as("sample_count")
        ).collect()(0)

        val avgAbsError = Option(errorStats.getAs[Double]("avg_abs_error")).getOrElse(0.0)
        val avgRelError = Option(errorStats.getAs[Double]("avg_rel_error")).getOrElse(0.0)
        val maxAbsError = Option(errorStats.getAs[Double]("max_abs_error")).getOrElse(0.0)
        val maxRelError = Option(errorStats.getAs[Double]("max_rel_error")).getOrElse(0.0)
        val errorStdDev = Option(errorStats.getAs[Double]("error_stddev")).getOrElse(0.0)
        val sampleCount = errorStats.getAs[Long]("sample_count")

        results += AccuracyResult(
          method = method,
          column = valueCol,
          percentile = p,
          avgAbsError = avgAbsError,
          avgRelError = avgRelError,
          maxAbsError = maxAbsError,
          maxRelError = maxRelError,
          errorStdDev = errorStdDev,
          sampleCount = sampleCount
        )

        errorDf.unpersist()

      } catch {
        case e: Exception =>
          println(s"计算 $method 在 $valueCol P${(p*100).toInt} 的误差时失败: ${e.getMessage}")
      }
    }

    results.toSeq
  }

  private def printAccuracyAnalysis(results: Seq[AccuracyResult]): Unit = {
    println("\n" + "="*100)
    println("精确性分析结果")
    println("="*100)

    printf("%-18s %-8s %-4s %-12s %-12s %-12s %-12s %-10s\n",
      "方法", "列", "分位", "平均绝对误差", "平均相对误差", "最大绝对误差", "最大相对误差", "样本数")
    println("-"*100)

    for (result <- results.sortBy(r => (r.method, r.column, r.percentile))) {
      printf("%-18s %-8s P%-2.0f %-12.6f %-12.6f %-12.6f %-12.6f %-10d\n",
        result.method,
        result.column,
        result.percentile * 100,
        result.avgAbsError,
        result.avgRelError,
        result.maxAbsError,
        result.maxRelError,
        result.sampleCount
      )
    }

    // 总体精确性总结
    println("\n" + "-"*100)
    println("总体精确性总结:")

    val methodGroups = results.groupBy(_.method)
    for ((method, methodResults) <- methodGroups) {
      val avgRelativeError = methodResults.map(_.avgRelError).sum / methodResults.length
      val maxRelativeError = methodResults.map(_.maxRelError).max
      val totalSamples = methodResults.map(_.sampleCount).sum

      printf("%-18s: 平均相对误差=%.4f%%, 最大相对误差=%.4f%%, 总样本数=%d\n",
        method, avgRelativeError * 100, maxRelativeError * 100, totalSamples)
    }

    println("="*100)
  }

  private def saveAccuracyResults(results: Seq[AccuracyResult], outputPath: String): Unit = {
    val file = new File(outputPath)
    file.getParentFile.mkdirs()

    val writer = new PrintWriter(file)
    try {
      writer.println("method,column,percentile,avg_abs_error,avg_rel_error,max_abs_error,max_rel_error,error_stddev,sample_count")
      for (result <- results) {
        writer.println(s"${result.method},${result.column},${result.percentile}," +
          s"${result.avgAbsError},${result.avgRelError},${result.maxAbsError}," +
          s"${result.maxRelError},${result.errorStdDev},${result.sampleCount}")
      }
    } finally {
      writer.close()
    }

    println(s"精确性测试结果已保存到: $outputPath")
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
