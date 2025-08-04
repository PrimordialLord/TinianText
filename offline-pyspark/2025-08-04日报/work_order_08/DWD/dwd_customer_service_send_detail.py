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

    # 构建动态SQL查询，用to_timestamp转换send_time和use_time为Timestamp类型
    select_sql1 = """
SELECT
    o.id,
    o.activity_id,
    o.product_id,
    CASE
        WHEN act.activity_level = '商品级' THEN NULL
        ELSE o.sku_id
        END AS sku_id,
    CASE
        WHEN o.customer_service_id <= 0 THEN cast(rand() * 100 + 100 AS BIGINT)
        ELSE o.customer_service_id
        END AS customer_service_id,
    CASE
        WHEN o.customer_id <= 0 THEN cast(rand() * 40000 + 10000 AS BIGINT)
        ELSE o.customer_id
        END AS customer_id,
    CASE
        WHEN act.activity_type = '自定义优惠' AND o.send_discount > act.max_discount
            THEN act.max_discount
        WHEN act.activity_type = '固定优惠' AND o.send_discount > prod.red_line_price * 0.9
            THEN prod.red_line_price * 0.9
        WHEN o.send_discount <= 0 THEN 10.00
        ELSE o.send_discount
        END AS send_discount,
    CASE
        WHEN o.valid_period < 1 THEN 24
        WHEN o.valid_period > 72 THEN 72
        ELSE o.valid_period
        END AS valid_period,
    -- 关键修复：用to_timestamp转换send_time为Timestamp
    to_timestamp(
        CASE
            WHEN unix_timestamp(o.send_time) < unix_timestamp(act.start_time)
                THEN from_unixtime(unix_timestamp(act.start_time))
            WHEN unix_timestamp(o.send_time) > unix_timestamp(act.end_time)
                THEN from_unixtime(unix_timestamp(act.end_time))
            ELSE from_unixtime(unix_timestamp(o.send_time))
        END,
        'yyyy-MM-dd HH:mm:ss'  -- 匹配时间字符串格式
    ) AS send_time,
    CASE
        WHEN o.remark IS NULL THEN '无备注'
        WHEN length(o.remark) > 50 THEN substr(o.remark, 1, 50)
        ELSE o.remark
        END AS remark,
    CASE
        WHEN o.is_used NOT IN (0, 1) THEN 0
        ELSE o.is_used
        END AS is_used,
    -- 关键修复：用to_timestamp转换use_time为Timestamp
    to_timestamp(
        CASE
            WHEN o.is_used = 0 THEN NULL
            WHEN unix_timestamp(o.use_time) < unix_timestamp(o.send_time)
                THEN from_unixtime(unix_timestamp(o.send_time))
            WHEN unix_timestamp(o.use_time) > unix_timestamp(o.send_time) + o.valid_period * 3600
                THEN from_unixtime(unix_timestamp(o.send_time) + o.valid_period * 3600)
            ELSE from_unixtime(unix_timestamp(o.use_time))
        END,
        'yyyy-MM-dd HH:mm:ss'  -- 匹配时间字符串格式
    ) AS use_time
FROM ods_customer_service_send_detail o
LEFT JOIN ods_activity_info act ON o.activity_id = act.activity_id
LEFT JOIN ods_product_info prod ON o.product_id = prod.product_id
WHERE act.activity_id IS NOT NULL
  AND prod.product_id IS NOT NULL
  AND NOT (act.activity_level = 'SKU级' AND o.sku_id IS NULL);
    """

    print(f"[INFO] 开始执行SQL查询")
    df1 = spark.sql(select_sql1)

    # 验证字段类型（确认send_time和use_time为timestamp）
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
    execute_hive_insert(target_date, 'dwd_customer_service_send_detail')