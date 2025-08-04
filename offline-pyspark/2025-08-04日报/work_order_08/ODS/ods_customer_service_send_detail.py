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

    # 构建动态SQL查询，修复row_number()需要ORDER BY的问题
    select_sql1 = """
SELECT id,
       activity_id,
       product_id,
       sku_id,
       customer_service_id,
       customer_id,
       send_discount,
       valid_period,
       send_time,
       remark,
       is_used,
       -- 使用时间（低版本Hive兼容写法：通过时间戳计算）
       CASE
           WHEN is_used = 1 THEN
               from_unixtime(
                       unix_timestamp(send_time) + cast(rand() * valid_period * 3600 AS INT)
               )
           ELSE NULL
           END AS use_time,
       -- 原始数据JSON串
       concat(
               '{"id":', id,
               ',"activity_id":', activity_id,
               ',"product_id":', product_id,
               ',"sku_id":', CASE WHEN sku_id IS NULL THEN 'null' ELSE sku_id END,
               ',"customer_service_id":', customer_service_id,
               ',"customer_id":', customer_id,
               ',"send_discount":', send_discount,
               ',"valid_period":', valid_period,
               ',"is_used":', is_used,
               '}'
       )       AS raw_data
FROM (SELECT row_number() OVER (ORDER BY pos)           AS id,  -- 添加ORDER BY pos排序
             cast(rand() * 100 + 1 AS BIGINT)          AS activity_id,
             cast(rand() * 1000 + 1000 AS BIGINT)      AS product_id,
             CASE
                 WHEN cast(rand() * 2 AS INT) = 0 THEN NULL
                 ELSE cast(rand() * 5000 + 10000 AS BIGINT)
                 END                                   AS sku_id,
             cast(rand() * 100 + 100 AS BIGINT)        AS customer_service_id,
             cast(rand() * 40000 + 10000 AS BIGINT)    AS customer_id,
             cast(rand() * 5000 + 1 AS DECIMAL(10, 2)) AS send_discount,
             cast(rand() * 23 + 1 AS INT)              AS valid_period,
             -- 发送时间：通过时间戳生成，兼容所有Hive版本
             from_unixtime(
                     unix_timestamp(current_date)
                         - cast(rand() * 30 * 86400 AS INT) -- 30天内随机天数
                         + cast(rand() * 86400 AS INT) -- 当天内随机秒数
             )                                         AS send_time,
             CASE
                 WHEN cast(rand() * 3 AS INT) = 0 THEN '新客转化'
                 WHEN cast(rand() * 3 AS INT) = 1 THEN '老客回馈'
                 ELSE NULL
                 END                                   AS remark,
             cast(rand() * 2 AS TINYINT)               AS is_used
      FROM (
               -- 生成1000条数据的基础序列
               SELECT pos
               FROM (SELECT posexplode(split(space(999), '')) AS (pos, val)) t) temp) data_source;
    """

    print(f"[INFO] 开始执行SQL查询")
    df1 = spark.sql(select_sql1)

    # 验证字段类型
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
    execute_hive_insert(target_date, 'ods_customer_service_send_detail')
