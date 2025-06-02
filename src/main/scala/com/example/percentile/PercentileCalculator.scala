package com.example.percentile

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types._
import scala.collection.mutable

/**
 * 修复UDF编码器错误的分位点计算器
 */
class PercentileCalculator(spark: SparkSession) {
  import spark.implicits._

  // 要计算的分位点
  val percentiles = Array(0.25, 0.5, 0.75, 0.9, 0.95, 0.99)
  val valueColumns = Array("value1", "value2", "value3", "value4", "value5")

  /**
   * 方法1: 使用approx_percentile (近似算法，高性能)
   */
  def calculateApproxPercentile(df: DataFrame): DataFrame = {
    println("开始计算 approx_percentile...")
    val startTime = System.currentTimeMillis()

    val aggExprs = for {
      col <- valueColumns
      p <- percentiles
    } yield {
      val colName = s"${col}_p${(p * 100).toInt}_approx"
      expr(s"approx_percentile($col, $p, 10000)").as(colName)
    }

    val result = df.groupBy("id").agg(aggExprs.head, aggExprs.tail: _*)

    val endTime = System.currentTimeMillis()
    println(s"approx_percentile 计算完成，耗时: ${endTime - startTime}ms")

    result
  }

  /**
   * 方法2: 使用percentile (另一种近似算法)
   */
  def calculatePercentile(df: DataFrame): DataFrame = {
    println("开始计算 percentile...")
    val startTime = System.currentTimeMillis()

    val aggExprs = for {
      col <- valueColumns
      p <- percentiles
    } yield {
      val colName = s"${col}_p${(p * 100).toInt}_approx2"
      expr(s"percentile($col, $p, 10000)").as(colName)
    }

    val result = df.groupBy("id").agg(aggExprs.head, aggExprs.tail: _*)

    val endTime = System.currentTimeMillis()
    println(s"percentile 计算完成，耗时: ${endTime - startTime}ms")

    result
  }

  /**
   * 方法3: 修复后的精确计算 - 使用Spark SQL的精确分位点函数
   */
  def calculateExactPercentile(df: DataFrame): DataFrame = {
    println("开始精确计算分位点（使用Spark SQL内置函数）...")
    val startTime = System.currentTimeMillis()

    // 使用Spark SQL内置的精确分位点函数
    val aggExprs = for {
      col <- valueColumns
      p <- percentiles
    } yield {
      val colName = s"${col}_p${(p * 100).toInt}_exact"
      expr(s"percentile($col, $p)").as(colName)
    }

    val result = df.groupBy("id").agg(aggExprs.head, aggExprs.tail: _*)

    val endTime = System.currentTimeMillis()
    println(s"精确计算完成，耗时: ${endTime - startTime}ms")

    result
  }

  /**
   * 方法4: 收集到Driver端计算（正确的实现，作为基准）
   */
  def calculateCollectPercentile(df: DataFrame): DataFrame = {
    println("开始collect计算分位点...")
    val startTime = System.currentTimeMillis()

    val groupedData = df.collect().groupBy(_.getAs[Int]("id"))
    val results = mutable.ArrayBuffer[(Int, Map[String, Double])]()

    for ((id, records) <- groupedData) {
      val percentileMap = mutable.Map[String, Double]()

      for (valueCol <- valueColumns) {
        val values = records.map(_.getAs[Double](valueCol)).sorted
        val n = values.length

        for (p <- percentiles) {
          // 正确的分位点计算实现（R-7方法）
          val position = p * (n - 1)  // 0-based position
          val index = position.toInt

          val value = if (index >= n - 1) {
            values.last
          } else {
            val lower = values(index)
            val upper = values(index + 1)
            val fraction = position - index
            lower + fraction * (upper - lower)  // 线性插值
          }

          percentileMap(s"${valueCol}_p${(p * 100).toInt}_collect") = value
        }
      }

      results += ((id, percentileMap.toMap))
    }

    // 转换回DataFrame
    val schema = StructType(
      StructField("id", IntegerType) +:
        (for {
          valueCol <- valueColumns
          p <- percentiles
        } yield StructField(s"${valueCol}_p${(p * 100).toInt}_collect", DoubleType)
          )
    )

    val rows = results.map { case (id, percentileMap) =>
      val values = id +: (for {
        valueCol <- valueColumns
        p <- percentiles
      } yield percentileMap(s"${valueCol}_p${(p * 100).toInt}_collect"))

      org.apache.spark.sql.Row(values: _*)
    }

    val result = spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)

    val endTime = System.currentTimeMillis()
    println(s"collect计算完成，耗时: ${endTime - startTime}ms")

    result
  }

  /**
   * 替代方案：使用collect_list而不是UDF的精确计算
   */
  def calculateExactPercentileAlternative(df: DataFrame): DataFrame = {
    println("开始精确计算分位点（collect_list方法）...")
    val startTime = System.currentTimeMillis()

    var result: DataFrame = df.select("id").distinct()

    for (valueCol <- valueColumns) {
      println(s"处理列: $valueCol")

      // 收集每组的所有值
      val collectedDf = df.groupBy("id")
        .agg(collect_list(valueCol).as("values"))

      for (p <- percentiles) {
        val colName = s"${valueCol}_p${(p * 100).toInt}_exact"

        // 使用Spark SQL表达式而不是UDF
        val percentileExpr = expr(s"""
          CASE
            WHEN size(values) = 0 THEN NULL
            WHEN size(values) = 1 THEN values[0]
            ELSE
              (sort_array(values)[CAST(${p} * (size(values) - 1) AS INT)] * (1 - (${p} * (size(values) - 1) - CAST(${p} * (size(values) - 1) AS INT))) +
               sort_array(values)[CAST(${p} * (size(values) - 1) AS INT) + 1] * (${p} * (size(values) - 1) - CAST(${p} * (size(values) - 1) AS INT)))
          END
        """).as(colName)

        val percentileDf = collectedDf.select(col("id"), percentileExpr)
        result = result.join(percentileDf, Seq("id"), "left")
      }
    }

    val endTime = System.currentTimeMillis()
    println(s"精确计算完成（collect_list方法），耗时: ${endTime - startTime}ms")

    result
  }

  /**
   * 验证和对比不同算法的结果
   */
  def compareAlgorithms(df: DataFrame): Unit = {
    println("\n" + "="*80)
    println("分位点算法对比验证")
    println("="*80)

    // 使用小数据集进行验证
    val testDf = df.sample(0.1).cache()

    try {
      // 计算各种方法的结果
      val collectResult = calculateCollectPercentile(testDf)
      val exactResult = calculateExactPercentile(testDf)  // 使用Spark SQL内置函数
      val approxResult = calculateApproxPercentile(testDf)

      // 显示第一个组的结果进行对比
      val collectRows = collectResult.where("id = (SELECT MIN(id) FROM collectResult)").collect()
      val exactRows = exactResult.where("id = (SELECT MIN(id) FROM exactResult)").collect()
      val approxRows = approxResult.where("id = (SELECT MIN(id) FROM approxResult)").collect()

      if (collectRows.nonEmpty && exactRows.nonEmpty && approxRows.nonEmpty) {
        println("\n算法对比结果:")
        println("-" * 80)
        printf("%-15s %-15s %-15s %-15s\n", "分位点", "Collect(基准)", "Exact(Spark)", "Approx")
        println("-" * 80)

        for (p <- percentiles) {
          val pStr = s"P${(p * 100).toInt}"
          val collectVal = collectRows(0).getAs[Double](s"value1_p${(p * 100).toInt}_collect")
          val exactVal = exactRows(0).getAs[Double](s"value1_p${(p * 100).toInt}_exact")
          val approxVal = approxRows(0).getAs[Double](s"value1_p${(p * 100).toInt}_approx")

          printf("%-15s %-15.2f %-15.2f %-15.2f\n", pStr, collectVal, exactVal, approxVal)

          // 验证算法一致性
          val diff = math.abs(collectVal - exactVal)
          if (diff > 0.01) {
            println(s"  ⚠️  注意: Exact算法与基准差异: $diff")
          }
        }

        println("-" * 80)
        println("✅ 算法对比完成")
      } else {
        println("⚠️  未找到足够的数据进行对比")
      }

    } catch {
      case e: Exception =>
        println(s"算法对比失败: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      testDf.unpersist()
    }
  }

  /**
   * 理论验证：计算1-100数据的理论分位点
   */
  def verifyTheoreticalPercentiles(): Unit = {
    println("\n" + "="*60)
    println("理论分位点验证 (1-100 数据)")
    println("="*60)

    val data = (1 to 100).map(_.toDouble).toArray

    printf("%-8s %-10s %-8s %-8s %-10s\n", "分位点", "位置", "下界", "上界", "理论值")
    println("-" * 60)

    for (p <- percentiles) {
      val position = p * (data.length - 1)  // 标准公式
      val index = position.toInt
      val fraction = position - index

      val lower = data(index)
      val upper = if (index >= data.length - 1) data.last else data(index + 1)
      val theoreticalValue = lower + fraction * (upper - lower)

      printf("P%-7.0f %-10.2f %-8.0f %-8.0f %-10.2f\n",
        p * 100, position, lower, upper, theoreticalValue)
    }

    println("-" * 60)
    println("说明: calculateCollectPercentile 的结果应该与理论值一致")
  }
}