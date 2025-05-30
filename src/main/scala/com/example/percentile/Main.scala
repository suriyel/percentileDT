package com.example.percentile

import org.apache.spark.sql.SparkSession

/**
 * 主程序入口
 */
object Main {

  def main(args: Array[String]): Unit = {
    // 解析命令行参数
    val numRecords = if (args.length > 0) {
      try {
        args(0).toLong
      } catch {
        case _: NumberFormatException =>
          println(s"无效的记录数: ${args(0)}，使用默认值 100000000")
          100000000L
      }
    } else {
      100000000L
    }

    println(s"将生成/使用 $numRecords 条记录进行测试")

    // 根据数据量调整Spark配置
    val (driverMemory, executorMemory, maxResultSize) = numRecords match {
      case n if n <= 1000000 => ("1g", "1g", "512m")      // 100万以下
      case n if n <= 10000000 => ("2g", "2g", "1g")       // 1000万以下
      case n if n <= 50000000 => ("4g", "4g", "2g")       // 5000万以下
      case _ => ("8g", "8g", "4g")                         // 更大数据量
    }

    // 创建Spark会话
    val spark = SparkSession.builder()
      .appName("Percentile Benchmark")
      .master("local[*]") // 使用所有可用核心
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.execution.arrow.pyspark.enabled", "true")
      .config("spark.driver.memory", driverMemory)
      .config("spark.executor.memory", executorMemory)
      .config("spark.driver.maxResultSize", maxResultSize)
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    try {
      val dataPath = s"data/test_data_${numRecords}.parquet"
      val resultPath = "results/benchmark_results.csv"

      // 检查是否需要生成数据
      val dataExists = try {
        val existingDf = spark.read.parquet(dataPath)
        val existingCount = existingDf.count()
        if (existingCount == numRecords) {
          println(s"找到匹配的数据文件，包含 $existingCount 条记录")
          true
        } else {
          println(s"数据文件记录数不匹配: $existingCount vs $numRecords")
          false
        }
      } catch {
        case _: Exception =>
          println("未找到数据文件")
          false
      }

      val df = if (!dataExists) {
        println("开始生成测试数据...")
        val generatedDf = DataGenerator.generateData(spark, numRecords)
        DataGenerator.saveAsParquet(generatedDf, dataPath)
        generatedDf
      } else {
        println("加载已有测试数据...")
        DataGenerator.loadParquet(spark, dataPath)
      }

      // 显示数据基本信息
      println("\n数据集信息:")
      df.printSchema()
      println(s"总行数: ${df.count()}")
      println(s"分组数: ${df.select("id").distinct().count()}")

      // 显示样本数据
      println("\n样本数据:")
      df.show(10)

      // 运行性能测试
      val benchmarkRunner = new BenchmarkRunner(spark)

      // 针对不同规模的数据进行测试
      val testSizes = Array(0.001, 0.01, 0.1, 1.0) // 0.1%, 1%, 10%, 100%

      for (sampleRate <- testSizes) {
        println(s"\n开始测试 ${(sampleRate * 100).formatted("%.1f")}% 数据量...")
        val testDf = if (sampleRate >= 1.0) df else df.sample(sampleRate)
        val testResultPath = s"results/benchmark_results_${(sampleRate * 100).toInt}pct.csv"

        benchmarkRunner.runBenchmark(testDf, testResultPath)
      }

      // 运行精确性测试
      benchmarkRunner.runAccuracyTest(df)

      println("\n所有测试完成！")

    } catch {
      case e: Exception =>
        e.printStackTrace()
        println(s"程序执行失败: ${e.getMessage}")
    } finally {
      spark.stop()
    }
  }
}