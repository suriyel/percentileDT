package com.example.percentile

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

// 测试数据结构定义 - 在类外避免导入问题
case class TestRecord(id: Int, value1: Double, value2: Double, value3: Double, value4: Double, value5: Double)

/**
 * 修正递归错误的测试套件
 */
class PercentileCalculatorTestSuite extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  var spark: SparkSession = _
  var calculator: PercentileCalculator = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("PercentileCalculatorTest")
      .master("local[2]")
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.driver.memory", "1g")
      .config("spark.ui.enabled", "false")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    calculator = new PercentileCalculator(spark)
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
  }

  /**
   * UT-001: 数据结构验证测试
   */
  "DataStructure" should "have correct schema and constraints" in {
    val sparkSession = spark
    import sparkSession.implicits._

    val testData = Seq(
      TestRecord(1, 10.0, 20.0, 30.0, 40.0, 50.0),
      TestRecord(2, 15.0, 25.0, 35.0, 45.0, 55.0)
    ).toDF()

    val expectedSchema = StructType(Array(
      StructField("id", IntegerType, false),
      StructField("value1", DoubleType, false),
      StructField("value2", DoubleType, false),
      StructField("value3", DoubleType, false),
      StructField("value4", DoubleType, false),
      StructField("value5", DoubleType, false)
    ))

    testData.schema shouldEqual expectedSchema
    testData.count() shouldEqual 2
    println("✅ UT-001: 数据结构验证通过")
  }

  /**
   * UT-002: 单组数据分位点计算验证
   */
  "PercentileCalculator" should "calculate correct percentiles for single group" in {
    val sparkSession = spark
    import sparkSession.implicits._

    val singleGroupData = (1 to 20).map(i =>
      TestRecord(1, i.toDouble, i.toDouble, i.toDouble, i.toDouble, i.toDouble)
    ).toDF()

    val result = calculator.calculateExactPercentile(singleGroupData)
    val collected = result.collect()(0)

    val expectedP50 = 10.0
    val actualP50 = collected.getAs[Double]("value1_p50_exact")

    math.abs(actualP50 - expectedP50) should be <= 2.0
    println(s"✅ UT-002: 单组分位点计算通过 - P50: $actualP50")
  }

  /**
   * UT-003: 多组数据分位点计算验证
   */
  "PercentileCalculator" should "calculate correct percentiles for multiple groups" in {
    val sparkSession = spark
    import sparkSession.implicits._

    val multiGroupData = Seq(
      TestRecord(1, 1.0, 1.0, 1.0, 1.0, 1.0),
      TestRecord(1, 5.0, 5.0, 5.0, 5.0, 5.0),
      TestRecord(1, 10.0, 10.0, 10.0, 10.0, 10.0),
      TestRecord(2, 11.0, 11.0, 11.0, 11.0, 11.0),
      TestRecord(2, 15.0, 15.0, 15.0, 15.0, 15.0),
      TestRecord(2, 20.0, 20.0, 20.0, 20.0, 20.0)
    ).toDF()

    val result = calculator.calculateExactPercentile(multiGroupData)
    result.count() shouldEqual 2

    val collected = result.collect()
    val group1 = collected.find(_.getAs[Int]("id") == 1).get
    val group2 = collected.find(_.getAs[Int]("id") == 2).get

    math.abs(group1.getAs[Double]("value1_p50_exact") - 5.0) should be <= 1.0
    math.abs(group2.getAs[Double]("value1_p50_exact") - 15.0) should be <= 1.0
    println("✅ UT-003: 多组分位点计算通过")
  }

  /**
   * UT-004: 近似算法精确性验证
   */
  "ApproxPercentile" should "have acceptable accuracy compared to exact calculation" in {
    val sparkSession = spark
    import sparkSession.implicits._

    val largeTestData = (1 to 1000).flatMap(i =>
      (1 to 10).map(j => TestRecord(i % 10, i.toDouble + j, i.toDouble + j,
        i.toDouble + j, i.toDouble + j, i.toDouble + j))
    ).toDF()

    val exactResult = calculator.calculateExactPercentile(largeTestData)
    val approxResult = calculator.calculateApproxPercentile(largeTestData)

    val joined = exactResult.join(approxResult, "id")
    val comparison = joined.withColumn("p50_error",
      abs(col("value1_p50_exact") - col("value1_p50_approx")) / col("value1_p50_exact")
    )

    val maxError = comparison.agg(max("p50_error")).collect()(0).getDouble(0)

    maxError should be <= 0.05
    println(s"✅ UT-004: 近似算法精确性通过 - 最大误差: ${(maxError*100).formatted("%.2f")}%")
  }

  /**
   * UT-005: 边界情况测试 - 单值组
   */
  "PercentileCalculator" should "handle single value groups correctly" in {
    val sparkSession = spark
    import sparkSession.implicits._

    val singleValueData = Seq(
      TestRecord(1, 100.0, 100.0, 100.0, 100.0, 100.0)
    ).toDF()

    val result = calculator.calculateExactPercentile(singleValueData)
    val collected = result.collect()(0)

    collected.getAs[Double]("value1_p25_exact") shouldEqual 100.0
    collected.getAs[Double]("value1_p50_exact") shouldEqual 100.0
    collected.getAs[Double]("value1_p75_exact") shouldEqual 100.0
    println("✅ UT-005: 单值组处理通过")
  }

  /**
   * UT-006: 边界情况测试 - 相同值组
   */
  "PercentileCalculator" should "handle groups with identical values" in {
    val sparkSession = spark
    import sparkSession.implicits._

    val identicalValueData = Seq(
      TestRecord(1, 50.0, 50.0, 50.0, 50.0, 50.0),
      TestRecord(1, 50.0, 50.0, 50.0, 50.0, 50.0),
      TestRecord(1, 50.0, 50.0, 50.0, 50.0, 50.0)
    ).toDF()

    val result = calculator.calculateExactPercentile(identicalValueData)
    val collected = result.collect()(0)

    collected.getAs[Double]("value1_p25_exact") shouldEqual 50.0
    collected.getAs[Double]("value1_p50_exact") shouldEqual 50.0
    collected.getAs[Double]("value1_p75_exact") shouldEqual 50.0
    println("✅ UT-006: 相同值组处理通过")
  }

  /**
   * UT-007: 边界情况测试 - 极值数据
   */
  "PercentileCalculator" should "handle extreme values correctly" in {
    val sparkSession = spark
    import sparkSession.implicits._

    val extremeValueData = Seq(
      TestRecord(1, -1000000.0, 0.0, 1.0, 10.0, 100.0),
      TestRecord(1, 0.0, 0.0, 1.0, 10.0, 100.0),
      TestRecord(1, 1000000.0, 0.0, 1.0, 10.0, 100.0)
    ).toDF()

    val result = calculator.calculateExactPercentile(extremeValueData)
    result.count() shouldEqual 1

    val collected = result.collect()(0)
    collected.getAs[Double]("value1_p50_exact") shouldEqual 0.0
    println("✅ UT-007: 极值数据处理通过")
  }

  /**
   * UT-008: 空数据处理测试
   */
  "PercentileCalculator" should "handle empty data gracefully" in {
    val sparkSession = spark
    import sparkSession.implicits._

    val emptyData = sparkSession.emptyDataFrame.select(
      lit(1).as("id"),
      lit(1.0).as("value1"),
      lit(1.0).as("value2"),
      lit(1.0).as("value3"),
      lit(1.0).as("value4"),
      lit(1.0).as("value5")
    ).filter(lit(false))

    val emptyResult = calculator.calculateExactPercentile(emptyData)
    emptyResult.count() shouldEqual 0
    println("✅ UT-008: 空数据处理通过")
  }

  /**
   * UT-009: 数据生成器验证测试
   */
  "DataGenerator" should "generate valid data structure" in {
    val generatedData = DataGenerator.generateData(spark, 1000L)

    generatedData.count() shouldEqual 1000L
    generatedData.columns should contain("id")
    generatedData.columns should contain("value1")

    val groupSizes = generatedData.groupBy("id").count()
    val maxGroupSize = groupSizes.agg(max("count")).collect()(0).getLong(0)

    maxGroupSize should be <= 200L
    println(s"✅ UT-009: 数据生成器验证通过 - 最大组大小: $maxGroupSize")
  }

  /**
   * UT-010: 性能基准测试
   */
  "PercentileCalculator" should "complete calculations within reasonable time" in {
    val sparkSession = spark
    import sparkSession.implicits._

    val performanceTestData = (1 to 5000).map(i =>
      TestRecord(i % 50, scala.util.Random.nextDouble() * 1000,
        scala.util.Random.nextDouble() * 1000,
        scala.util.Random.nextDouble() * 1000,
        scala.util.Random.nextDouble() * 1000,
        scala.util.Random.nextDouble() * 1000)
    ).toDF()

    val startTime = System.currentTimeMillis()
    val result = calculator.calculateApproxPercentile(performanceTestData)
    result.count()
    val endTime = System.currentTimeMillis()

    val executionTime = endTime - startTime

    executionTime should be <= 15000L // 15秒
    result.count() shouldEqual 50
    println(s"✅ UT-010: 性能测试通过 - 执行时间: ${executionTime}ms")
  }

  /**
   * UT-011: 算法一致性验证测试
   */
  "Different percentile algorithms" should "produce consistent results" in {
    val sparkSession = spark
    import sparkSession.implicits._

    val consistencyTestData = (1 to 100).map(i =>
      TestRecord(1, i.toDouble, i.toDouble, i.toDouble, i.toDouble, i.toDouble)
    ).toDF()

    val exactResult = calculator.calculatePercentile(consistencyTestData)
    val collectResult = calculator.calculateCollectPercentile(consistencyTestData)

    val exactP50 = exactResult.collect()(0).getAs[Double]("value2_p50_exact")
    val collectP50 = collectResult.collect()(0).getAs[Double]("value1_p50_collect")

    math.abs(exactP50 - collectP50) should be <= 0.1
    println(s"✅ UT-011: 算法一致性验证通过 - 精确值: $exactP50, Collect值: $collectP50")
  }

  /**
   * UT-012: 数据分布验证测试 - 修正递归错误
   */
  "Generated data" should "follow expected statistical distributions" in {
    val generatedData = DataGenerator.generateData(spark, 10000L)

    // 修正：避免变量名与函数名冲突
    val statisticsResult = generatedData.agg(
      avg("value1").as("mean_value"),
      stddev("value1").as("stddev_value"),  // 重命名避免冲突
      min("value1").as("min_value"),
      max("value1").as("max_value")
    ).collect()(0)

    val meanValue = statisticsResult.getAs[Double]("mean_value")
    val stddevValue = statisticsResult.getAs[Double]("stddev_value")  // 使用不同的变量名

    // 验证统计特性
    math.abs(meanValue - 500.0) should be <= 20.0
    math.abs(stddevValue - 100.0) should be <= 30.0

    println(s"✅ UT-012: 数据分布验证通过 - 均值: ${meanValue.formatted("%.2f")}, 标准差: ${stddevValue.formatted("%.2f")}")
  }
}

/**
 * 简化的性能基准测试类
 */
class PerformanceBenchmarkTest extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("PerformanceBenchmarkTest")
      .master("local[2]")
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.driver.memory", "2g")
      .config("spark.ui.enabled", "false")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
  }

  "Performance benchmark" should "compare algorithm execution times" in {
    val calculator = new PercentileCalculator(spark)
    val sparkSession = spark
    import sparkSession.implicits._

    val testData = (1 to 5000).map(i =>
      TestRecord(i % 50, scala.util.Random.nextDouble() * 1000,
        scala.util.Random.nextDouble() * 1000,
        scala.util.Random.nextDouble() * 1000,
        scala.util.Random.nextDouble() * 1000,
        scala.util.Random.nextDouble() * 1000)
    ).toDF()

    val approxStartTime = System.currentTimeMillis()
    val approxResult = calculator.calculateApproxPercentile(testData)
    approxResult.count()
    val approxEndTime = System.currentTimeMillis()
    val approxTime = approxEndTime - approxStartTime

    println(s"✅ 性能基准测试 - approx_percentile: ${approxTime}ms")

    approxTime should be <= 30000L
    approxResult.count() shouldEqual 50
  }
}