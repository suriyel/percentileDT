package com.example.percentile

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types._
import scala.collection.mutable

/**
 * 分位点计算器，实现多种算法
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

    // 构建聚合表达式
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
   * 方法2: 使用percentile_approx (另一种近似算法)
   */
  def calculatePercentileApprox(df: DataFrame): DataFrame = {
    println("开始计算 percentile_approx...")
    val startTime = System.currentTimeMillis()

    // 构建聚合表达式
    val aggExprs = for {
      col <- valueColumns
      p <- percentiles
    } yield {
      val colName = s"${col}_p${(p * 100).toInt}_approx2"
      expr(s"percentile_approx($col, $p, 10000)").as(colName)
    }

    val result = df.groupBy("id").agg(aggExprs.head, aggExprs.tail: _*)

    val endTime = System.currentTimeMillis()
    println(s"percentile_approx 计算完成，耗时: ${endTime - startTime}ms")

    result
  }

  /**
   * 方法3: 精确计算（使用窗口函数）
   */
  def calculateExactPercentile(df: DataFrame): DataFrame = {
    println("开始精确计算分位点...")
    val startTime = System.currentTimeMillis()

    // 为每个value列计算精确分位点
    var result: DataFrame = df.select("id").distinct()

    for (valueCol <- valueColumns) {
      println(s"处理列: $valueCol")

      val windowSpec = Window.partitionBy("id").orderBy(valueCol)
      val countWindow = Window.partitionBy("id")

      val rankedDf = df
        .withColumn("rn", row_number().over(windowSpec))
        .withColumn("total_count", count("*").over(countWindow))
        .withColumn("percentile_rank",
          col("rn").cast("double") / col("total_count").cast("double"))

      for (p <- percentiles) {
        val colName = s"${valueCol}_p${(p * 100).toInt}_exact"

        val percentileDf = rankedDf
          .filter(col("percentile_rank") >= p)
          .groupBy("id")
          .agg(min(valueCol).as(colName))

        result = result.join(percentileDf, Seq("id"), "left")
      }
    }

    val endTime = System.currentTimeMillis()
    println(s"精确计算完成，耗时: ${endTime - startTime}ms")

    result
  }

  /**
   * 方法4: 收集到Driver端计算（适用于小数据集）
   */
  def calculateCollectPercentile(df: DataFrame): DataFrame = {
    println("开始collect计算分位点...")
    val startTime = System.currentTimeMillis()

    // 收集数据到Driver
    val groupedData = df.collect().groupBy(_.getAs[Int]("id"))

    val results = mutable.ArrayBuffer[(Int, Map[String, Double])]()

    for ((id, records) <- groupedData) {
      val percentileMap = mutable.Map[String, Double]()

      for (valueCol <- valueColumns) {
        val values = records.map(_.getAs[Double](valueCol)).sorted
        val n = values.length

        for (p <- percentiles) {
          val index = (p * (n - 1)).toInt
          val value = if (index >= n - 1) values.last else {
            val lower = values(index)
            val upper = values(index + 1)
            val fraction = (p * (n - 1)) - index
            lower + fraction * (upper - lower)
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
}
