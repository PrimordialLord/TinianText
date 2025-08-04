from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

def get_spark_session():
    """初始化SparkSession并配置Hive连接，确保数据库存在"""
    spark = SparkSession.builder \
        .appName("HiveETL") \
        .config("spark.hadoop.hive.metastore.uris", "thrift://cdh01:9083") \
        .config("spark.sql.hive.convertMetastoreOrc", "true") \
        .config("spark.sql.orc.compression.codec", "snappy")   \
        .config("hive.exec.dynamic.partition.mode", "nonstrict") \
        .config("spark.sql.hive.ignoreMissingPartitions", "true") \
        .enableHiveSupport() \
        .getOrCreate()

    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    spark.sql("CREATE DATABASE IF NOT EXISTS work_order_07")
    spark.sql("USE work_order_07")

    return spark


def select_to_hive(jdbcDF, tableName):
    """将DataFrame数据追加写入Hive表"""
    jdbcDF.write.mode('append').insertInto(f"{tableName}")


def execute_hive_insert(partition_date: str, tableName):
    """从源表查询数据并写入目标Hive分区表"""
    spark = get_spark_session()

    # 构建动态SQL查询，修复了字符串格式化问题
    select_sql1 = """
SELECT
    -- 商品ID（与商品基础表关联，确保非空）
    opb.product_id,

    -- 访客数（清洗异常值：确保≥0）
    case when opb.visitor_count < 0 then 0 else opb.visitor_count end           AS visitor_count,

    -- 销售金额（清洗异常值：确保≥0，保留两位小数）
    round(case when opb.sales_amount < 0 then 0 else opb.sales_amount end, 2)   AS sales_amount,

    -- 销量（清洗异常值：确保≥0）
    case when opb.sales_volume < 0 then 0 else opb.sales_volume end             AS sales_volume,

    -- 转化率（清洗异常值：限制在0-100%，保留两位小数）
    round(
            case
                when opb.conversion_rate < 0 then 0
                when opb.conversion_rate > 100 then 100
                else opb.conversion_rate
                end, 2
    )                                                                           AS conversion_rate,

    -- 新增客户数（清洗异常值：确保≥0）
    case when opb.new_customer_count < 0 then 0 else opb.new_customer_count end AS new_customer_count,

    -- 流量指标（清洗异常值：限制在0-100）
    round(
            case
                when opb.traffic_index < 0 then 0
                when opb.traffic_index > 100 then 100
                else opb.traffic_index
                end, 2
    )                                                                           AS traffic_index,

    -- 转化指标（清洗异常值：限制在0-100）
    round(
            case
                when opb.conversion_index < 0 then 0
                when opb.conversion_index > 100 then 100
                else opb.conversion_index
                end, 2
    )                                                                           AS conversion_index,

    -- 内容指标（清洗异常值：限制在0-100）
    round(
            case
                when opb.content_index < 0 then 0
                when opb.content_index > 100 then 100
                else opb.content_index
                end, 2
    )                                                                           AS content_index,

    -- 拉新指标（清洗异常值：限制在0-100）
    round(
            case
                when opb.new_customer_index < 0 then 0
                when opb.new_customer_index > 100 then 100
                else opb.new_customer_index
                end, 2
    )                                                                           AS new_customer_index,

    -- 服务指标（清洗异常值：限制在0-100）
    round(
            case
                when opb.service_index < 0 then 0
                when opb.service_index > 100 then 100
                else opb.service_index
                end, 2
    )                                                                           AS service_index,

    -- 记录DWD层ETL处理时间
    from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss')                      AS etl_time
FROM
    -- 源表：ODS层商品行为表（指定分区）
    ods_product_behavior opb
WHERE
  -- 过滤当前分区数据
    opb.dt = '2025-08-03'
  -- 核心过滤：排除商品ID为空的无效数据
  AND opb.product_id IS NOT NULL
  AND opb.product_id != ''
  -- 业务校验：访客数不能为负数（基础过滤）
  AND opb.visitor_count >= 0;
    """

    print(f"[INFO] 开始执行SQL查询，目标分区：{partition_date}")
    df1 = spark.sql(select_sql1)

    # 添加分区字段ds
    df_with_partition = df1.withColumn("dt", lit(partition_date))

    print(f"[INFO] SQL执行完成，分区{partition_date}操作成功")
    df_with_partition.show(5)

    # 写入数据
    select_to_hive(df_with_partition, tableName)

    # 验证数据
    print(f"[INFO] 验证分区 {partition_date} 的数据...")
    verify_df = spark.sql(f"SELECT * FROM {tableName} WHERE dt='{partition_date}' LIMIT 5")
    verify_df.show()


if __name__ == "__main__":
    target_date = '2025-08-03'
    execute_hive_insert(target_date, 'dwd_product_behavior_detail')
