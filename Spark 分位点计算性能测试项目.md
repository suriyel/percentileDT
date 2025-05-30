# Spark 分位点计算性能测试项目

## 项目概述
本项目实现了基于Apache Spark的海量数据分位点计算，支持多种算法并提供性能对比测试。支持从百万级到亿级数据量的分位点计算性能测试。

## 功能特性
- 灵活的数据量配置（默认1亿条，可调整）
- 支持4种分位点计算方法：
  - `approx_percentile`（Spark内置近似算法，基于KLL sketch）
  - `percentile_approx`（另一种近似算法实现）
  - 精确计算（基于窗口函数排序）
  - 本地收集计算（适用于小数据集验证）
- 智能性能测试（根据数据量自动选择合适算法）
- 多规模测试（0.1%, 1%, 10%, 100%数据量对比）
- 精确性对比分析
- 自动内存配置优化
- CSV格式结果导出

## 环境要求
- **操作系统**: Windows 10/11 (64位)
- **Java**: Java 8 或 Java 11 (推荐OpenJDK)
- **Scala**: 2.12.x (通过SBT自动管理)
- **SBT**: 1.8+ 
- **内存**: 至少8GB RAM (16GB推荐用于大数据量测试)
- **磁盘空间**: 至少20GB可用空间

## 安装步骤

### 1. 安装Java
下载并安装OpenJDK 11:
```
https://adoptium.net/temurin/releases/
```

### 2. 安装SBT
从官网下载SBT安装包:
```
https://www.scala-sbt.org/download.html
```

### 3. 验证安装
运行环境检查脚本:
```cmd
setup.bat
```

## 快速开始

### 方式1: 完整测试 (1亿条数据)
```cmd
run.bat
```

### 方式2: 快速验证 (100万条数据)
```cmd
quick-test.bat
```

### 方式3: 自定义数据量
```cmd
sbt compile
sbt "run 10000000"    # 1000万条数据
sbt "run 50000000"    # 5000万条数据
```

## 使用说明

### 命令行参数
```cmd
sbt "run [记录数]"
```
- 不指定参数：默认1亿条记录
- 指定记录数：生成指定数量的测试数据

### 内存配置自动调整
程序会根据数据量自动调整Spark内存配置：
- ≤100万: 1GB driver + 1GB executor
- ≤1000万: 2GB driver + 2GB executor  
- ≤5000万: 4GB driver + 4GB executor
- >5000万: 8GB driver + 8GB executor

### 输出文件
```
data/
├── test_data_[记录数].parquet    # 生成的测试数据
results/
├── benchmark_results_1pct.csv   # 1%数据量测试结果
├── benchmark_results_10pct.csv  # 10%数据量测试结果
├── benchmark_results_100pct.csv # 全量数据测试结果
└── ...
```

## 项目结构
```
spark-percentile-benchmark/
├── build.sbt                          # SBT构建配置
├── project/
│   ├── build.properties               # SBT版本配置
│   └── plugins.sbt                    # SBT插件配置
├── src/main/scala/com/example/percentile/
│   ├── DataGenerator.scala            # 测试数据生成器
│   ├── PercentileCalculator.scala     # 分位点计算实现
│   ├── BenchmarkRunner.scala          # 性能测试运行器
│   ├── QuickTest.scala               # 快速测试程序
│   └── Main.scala                    # 主程序入口
├── data/                             # 数据存储目录
├── results/                          # 结果输出目录
├── run.bat                          # Windows运行脚本
├── quick-test.bat                   # 快速测试脚本
├── setup.bat                       # 环境检查脚本
└── README.md                        # 项目说明
```

## 算法详解

### 1. approx_percentile
- **原理**: 基于KLL sketch数据结构的近似算法
- **优势**: 极高性能，内存占用恒定，适合超大数据集
- **精度**: 可配置，默认相对误差 < 1%
- **适用场景**: 生产环境大数据分位点计算

### 2. percentile_approx  
- **原理**: T-Digest算法的变种实现
- **优势**: 在某些数据分布下精度更高
- **性能**: 与approx_percentile相近
- **适用场景**: 需要稍高精度的近似计算

### 3. 精确计算
- **原理**: 基于窗口函数的完全排序
- **优势**: 100%精确结果
- **劣势**: 性能较低，内存占用大
- **适用场景**: 中小数据集、结果验证

### 4. 本地收集计算
- **原理**: 将数据收集到Driver节点排序
- **优势**: 实现简单，结果精确
- **限制**: 仅适用于小数据集
- **适用场景**: 算法验证、小规模测试

## 性能调优

### JVM参数优化
如果遇到内存问题，可以手动调整JVM参数：
```cmd
set JAVA_OPTS=-Xmx12g -XX:+UseG1GC
sbt run
```

### Spark参数调整
修改Main.scala中的Spark配置：
```scala
.config("spark.driver.memory", "16g")           # 增加driver内存
.config("spark.executor.memory", "16g")         # 增加executor内存
.config("spark.sql.shuffle.partitions", "400")  # 调整分区数
```

### 硬件建议
- **CPU**: 至少4核心，8核心以上推荐
- **内存**: 16GB以上推荐
- **存储**: SSD硬盘提升I/O性能

## 测试结果解读

### 性能指标
- **执行时间**: 各算法的端到端计算耗时
- **结果行数**: 输出的分组数量
- **平均组大小**: 每个ID分组的平均记录数
- **吞吐量**: 每秒处理的记录数

### 精确性指标
- **相对误差**: (近似值 - 精确值) / 精确值
- **绝对误差**: |近似值 - 精确值|
- **误差分布**: 不同分位点的误差统计

## 故障排除

### 常见问题

1. **内存不足错误**
```
Exception: Java heap space
```
**解决方案**: 
- 减少数据量测试
- 增加JVM内存: `set JAVA_OPTS=-Xmx16g`
- 调整Spark内存配置

2. **编译失败**
```
Error: Could not find or load main class
```
**解决方案**:
- 检查Java版本: `java -version`
- 重新编译: `sbt clean compile`
- 检查JAVA_HOME环境变量

3. **SBT下载慢**
```
Downloading dependencies...
```
**解决方案**:
- 使用国内镜像源
- 配置代理
- 检查网络连接

4. **Parquet文件损坏**
```
Exception: Corrupted parquet file
```
**解决方案**:
- 删除data目录重新生成
- 检查磁盘空间
- 使用小数据量测试

### 性能问题排查

1. **程序运行很慢**
- 检查可用内存
- 减少数据量进行测试
- 监控CPU和磁盘使用率

2. **内存使用过高**
- 调整批处理大小
- 使用采样数据测试
- 增加系统内存

3. **磁盘空间不足**
- 清理临时文件
- 使用压缩存储
- 选择更大的磁盘

## 扩展开发

### 添加新算法
1. 在`PercentileCalculator`中添加新方法
2. 更新`BenchmarkRunner`调用新算法
3. 修改结果输出格式

### 支持新数据类型
1. 修改`TestRecord`结构
2. 更新数据生成逻辑
3. 调整分位点计算

### 集群部署
1. 修改`.master("local[*]")`为集群地址
2. 调整内存和核心配置
3. 配置HDFS路径

## 技术支持
- 项目问题: 检查GitHub Issues
- Spark文档: https://spark.apache.org/docs/latest/
- Scala文档: https://docs.scala-lang.org/

## 许可证
MIT License