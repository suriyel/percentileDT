package com.example.percentile

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import scala.util.Random

/**
 * 数据生成器，生成1亿条测试数据
 */
object DataGenerator {

  case class TestRecord(
                         id: Int,
                         value1: Double,
                         value2: Double,
                         value3: Double,
                         value4: Double,
                         value5: Double
                       )

  def generateData(spark: SparkSession, numRecords: Long = 100000000L): DataFrame = {
    import spark.implicits._

    println(s"开始生成 $numRecords 条测试数据...")

    // 生成数据的参数
    val maxGroups = (numRecords / 100).toInt  // 平均每组100条数据，确保每组不超过200条
    val seed = 12345L
    val numPartitions = math.min(200, math.max(4, (numRecords / 1000000).toInt)) // 动态分区数

    println(s"使用 $numPartitions 个分区生成数据")

    // 使用Spark的range和map来生成大量数据
    val df = spark.range(0, numRecords, 1, numPartitions)
      .mapPartitions { iter =>
        val partitionId = org.apache.spark.TaskContext.getPartitionId()
        val random = new Random(seed + partitionId)
        iter.map { i =>
          val id = (random.nextDouble() * maxGroups).toInt
          TestRecord(
            id = id,
            value1 = random.nextGaussian() * 100 + 500,  // 正态分布：均值500，标准差100
            value2 = random.nextDouble() * 1000,          // 均匀分布：0-1000
            value3 = math.exp(random.nextGaussian()),     // 对数正态分布
            value4 = random.nextDouble() * 50 + 25,       // 均匀分布：25-75
            value5 = if (random.nextDouble() < 0.1)
              random.nextDouble() * 10000 + 1000   // 10%异常值：1000-11000
            else
              random.nextDouble() * 100             // 90%正常值：0-100
          )
        }
      }.toDF()

    println("数据生成完成，开始缓存和重分区...")

    // 缓存并重分区以优化后续查询性能
    val result = df.repartition(col("id")).cache()

    // 触发缓存
    println(s"生成的数据总行数: ${result.count()}")

    result
  }

  def saveAsParquet(df: DataFrame, path: String): Unit = {
    println(s"保存数据到: $path")
    df.write
      .mode("overwrite")
      .option("compression", "snappy")
      .parquet(path)
    println("数据保存完成")
  }

  def loadParquet(spark: SparkSession, path: String): DataFrame = {
    println(s"从 $path 加载数据...")
    spark.read.parquet(path)
  }
}