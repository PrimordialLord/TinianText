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

    spark.sql("CREATE DATABASE IF NOT EXISTS work_order_08")
    spark.sql("USE work_order_08")

    return spark


def select_to_hive(jdbcDF, tableName):
    """将DataFrame数据追加写入Hive表"""
    jdbcDF.write.mode('append').insertInto(f"{tableName}")


def execute_hive_insert(partition_date: str, tableName):
    """从源表查询数据并写入目标Hive分区表"""
    spark = get_spark_session()

    # 构建动态SQL查询，用to_timestamp转换add_time和remove_time为Timestamp类型
    select_sql1 = """
SELECT
    rel.id,
    rel.activity_id,
    rel.product_id,
    CASE
        WHEN act.activity_level = '商品级' THEN NULL
        ELSE rel.sku_id
        END AS sku_id,
    CASE
        WHEN act.activity_type = '固定优惠' THEN
            CASE
                WHEN rel.discount_amount <= 0 THEN 10.00
                WHEN rel.discount_amount > prod.red_line_price THEN prod.red_line_price * 0.9
                ELSE rel.discount_amount
                END
        ELSE NULL
        END AS discount_amount,
    CASE
        WHEN rel.limit_purchase < 1 THEN 1
        WHEN rel.limit_purchase > 5 THEN 5
        ELSE rel.limit_purchase
        END AS limit_purchase,
    CASE
        WHEN rel.is_published NOT IN (0, 1) THEN 0
        ELSE rel.is_published
        END AS is_published,
    -- 关键修复：用to_timestamp转换add_time为Timestamp
    to_timestamp(
        CASE
            WHEN unix_timestamp(rel.add_time) < unix_timestamp(act.start_time)
                THEN from_unixtime(unix_timestamp(act.start_time))
            ELSE from_unixtime(unix_timestamp(rel.add_time))
        END,
        'yyyy-MM-dd HH:mm:ss'  -- 匹配时间字符串格式
    ) AS add_time,
    -- 关键修复：用to_timestamp转换remove_time为Timestamp
    to_timestamp(
        CASE
            WHEN rel.is_published = 1 THEN NULL
            WHEN rel.remove_time IS NOT NULL AND unix_timestamp(rel.remove_time) < unix_timestamp(rel.add_time)
                THEN from_unixtime(unix_timestamp(rel.add_time))
            ELSE from_unixtime(unix_timestamp(rel.remove_time))
        END,
        'yyyy-MM-dd HH:mm:ss'  -- 匹配时间字符串格式
    ) AS remove_time
FROM ods_product_activity_rel rel
LEFT JOIN ods_activity_info act ON rel.activity_id = act.activity_id
LEFT JOIN ods_product_info prod ON rel.product_id = prod.product_id
WHERE act.activity_id IS NOT NULL
  AND prod.product_id IS NOT NULL
  AND NOT (act.activity_level = 'SKU级' AND rel.sku_id IS NULL);
    """

    print(f"[INFO] 开始执行SQL查询")
    df1 = spark.sql(select_sql1)

    # 验证字段类型（确认add_time和remove_time为timestamp）
    print("[INFO] DataFrame字段类型：")
    df1.printSchema()
    print(f"[INFO] DataFrame列数: {len(df1.columns)}")

    print(f"[INFO] SQL执行完成")
    df1.show(5)

    # 写入数据
    select_to_hive(df1, tableName)

    # 验证数据
    print(f"[INFO] 验证数据...")
    verify_df = spark.sql(f"SELECT * FROM {tableName} LIMIT 5")
    verify_df.show()


if __name__ == "__main__":
    target_date = '2025-08-03'
    execute_hive_insert(target_date, 'dwd_product_activity_rel')