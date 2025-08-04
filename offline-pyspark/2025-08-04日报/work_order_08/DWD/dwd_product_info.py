from pyspark.sql import SparkSession
from pyspark.sql.functions import lit  # 无需额外导入to_timestamp，SQL中直接使用

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

    # 构建动态SQL查询，关键：用to_timestamp转换时间为Timestamp类型
    select_sql1 = """
SELECT
    o.product_id,
    -- 商品名称清洗
    CASE
        WHEN o.product_name IS NULL THEN CONCAT('未知商品_', o.product_id)
        WHEN LENGTH(o.product_name) > 100 THEN SUBSTR(o.product_name, 1, 100)
        ELSE REGEXP_REPLACE(o.product_name, '[^a-zA-Z0-9\u4e00-\u9fa5]', '')
        END AS product_name,
    -- 商品价格清洗
    CASE
        WHEN o.price <= 0 THEN 99.99
        WHEN o.price > 10000 THEN 10000.00
        ELSE o.price
        END AS price,
    -- 红线价清洗
    CASE
        WHEN o.red_line_price > price THEN ROUND(price * 0.4, 2)
        WHEN o.red_line_price <= 0 THEN ROUND(price * 0.4, 2)
        ELSE o.red_line_price
        END AS red_line_price,
    -- 关键修复：用to_timestamp转换为Timestamp，指定原始格式为yyyy-MM-dd
    to_timestamp(
        CASE
            WHEN REGEXP_EXTRACT(o.create_time, '\\d{4}-\\d{2}-\\d{2}', 0) = ''
                THEN FROM_UNIXTIME(UNIX_TIMESTAMP()) -- 无效时间用当前时间
            ELSE FROM_UNIXTIME(UNIX_TIMESTAMP(o.create_time, 'yyyy-MM-dd'))
        END,
        'yyyy-MM-dd HH:mm:ss'  -- 匹配转换后的时间格式
    ) AS create_time,
    -- 关键修复：同理转换update_time
    to_timestamp(
        CASE
            WHEN UNIX_TIMESTAMP(o.update_time, 'yyyy-MM-dd') < UNIX_TIMESTAMP(o.create_time, 'yyyy-MM-dd')
                THEN FROM_UNIXTIME(UNIX_TIMESTAMP(o.create_time, 'yyyy-MM-dd'))
            WHEN REGEXP_EXTRACT(o.update_time, '\\d{4}-\\d{2}-\\d{2}', 0) = ''
                THEN FROM_UNIXTIME(UNIX_TIMESTAMP(o.create_time, 'yyyy-MM-dd'))
            ELSE FROM_UNIXTIME(UNIX_TIMESTAMP(o.update_time, 'yyyy-MM-dd'))
        END,
        'yyyy-MM-dd HH:mm:ss'
    ) AS update_time
FROM ods_product_info o
WHERE o.product_id IS NOT NULL AND o.product_id > 0;
    """

    print(f"[INFO] 开始执行SQL查询")
    df1 = spark.sql(select_sql1)  # 此时create_time和update_time为Timestamp类型

    # 不添加dt字段，保持6列与目标表一致
    df_without_partition = df1

    # 验证字段类型（关键：确认create_time和update_time为timestamp）
    print("[INFO] DataFrame字段类型：")
    df_without_partition.printSchema()
    print(f"[INFO] DataFrame列数: {len(df_without_partition.columns)}")  # 应输出6

    print(f"[INFO] SQL执行完成")
    df_without_partition.show(5)

    # 写入数据
    select_to_hive(df_without_partition, tableName)

    # 验证数据
    print(f"[INFO] 验证数据...")
    verify_df = spark.sql(f"SELECT * FROM {tableName} LIMIT 5")
    verify_df.show()


if __name__ == "__main__":
    target_date = '2025-08-03'
    execute_hive_insert(target_date, 'dwd_product_info')