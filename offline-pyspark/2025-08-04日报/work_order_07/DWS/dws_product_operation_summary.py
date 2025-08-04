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
    -- 1. product_id
    opb.product_id,
    -- 2. visitor_count
    case when opb.visitor_count < 0 then 0 else opb.visitor_count end,
    -- 3. sales_amount
    round(case when opb.sales_amount < 0 then 0 else opb.sales_amount end, 2),
    -- 4. sales_volume
    case when opb.sales_volume < 0 then 0 else opb.sales_volume end,
    -- 5. conversion_rate
    round(case when opb.conversion_rate < 0 then 0 when opb.conversion_rate > 100 then 100 else opb.conversion_rate end, 2),
    -- 6. new_customer_count
    case when opb.new_customer_count < 0 then 0 else opb.new_customer_count end,
    -- 7. traffic_index
    round(case when opb.traffic_index < 0 then 0 when opb.traffic_index > 100 then 100 else opb.traffic_index end, 2),
    -- 8. conversion_index
    round(case when opb.conversion_index < 0 then 0 when opb.conversion_index > 100 then 100 else opb.conversion_index end, 2),
    -- 9. content_index
    round(case when opb.content_index < 0 then 0 when opb.content_index > 100 then 100 else opb.content_index end, 2),
    -- 10. new_customer_index
    round(case when opb.new_customer_index < 0 then 0 when opb.new_customer_index > 100 then 100 else opb.new_customer_index end, 2),
    -- 11. service_index
    round(case when opb.service_index < 0 then 0 when opb.service_index > 100 then 100 else opb.service_index end, 2),
    -- 12. etl_time
    from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss')
FROM
    ods_product_behavior opb
WHERE
    opb.dt = '2025-08-03';
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
