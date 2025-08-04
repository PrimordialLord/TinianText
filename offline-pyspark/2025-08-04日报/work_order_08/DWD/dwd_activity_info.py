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

    # 构建动态SQL查询，用to_timestamp转换所有时间字段为Timestamp类型
    select_sql1 = """
SELECT
    o.activity_id,
    -- 清洗活动名称
    CASE
        WHEN length(o.activity_name) > 10 THEN substr(o.activity_name, 1, 10)
        ELSE o.activity_name
        END AS activity_name,
    -- 标准化活动级别
    CASE
        WHEN o.activity_level NOT IN ('商品级', 'SKU级') THEN '商品级'
        ELSE o.activity_level
        END AS activity_level,
    -- 标准化优惠类型
    CASE
        WHEN o.activity_type NOT IN ('固定优惠', '自定义优惠') THEN '固定优惠'
        ELSE o.activity_type
        END AS activity_type,
    -- 转换开始时间为TIMESTAMP（关键修复）
    to_timestamp(
        CASE
            WHEN regexp_extract(o.start_time, '\\d{4}-\\d{2}-\\d{2}', 0) = ''
                THEN from_unixtime(unix_timestamp()) -- 无效时间用当前时间
            ELSE from_unixtime(unix_timestamp(o.start_time, 'yyyy-MM-dd'))
        END,
        'yyyy-MM-dd HH:mm:ss'  -- 匹配时间格式
    ) AS start_time,
    -- 转换结束时间为TIMESTAMP（关键修复）
    to_timestamp(
        CASE
            WHEN unix_timestamp(o.end_time, 'yyyy-MM-dd') < unix_timestamp(o.start_time, 'yyyy-MM-dd')
                THEN from_unixtime(unix_timestamp(o.start_time, 'yyyy-MM-dd') + 30 * 86400)
            ELSE from_unixtime(unix_timestamp(o.end_time, 'yyyy-MM-dd'))
        END,
        'yyyy-MM-dd HH:mm:ss'
    ) AS end_time,
    -- 清洗最大优惠金额
    CASE
        WHEN o.activity_type = '自定义优惠'
            THEN CASE
                     WHEN o.max_discount IS NULL OR o.max_discount <= 0 THEN 100.00
                     WHEN o.max_discount > 5000 THEN 5000.00
                     ELSE o.max_discount
            END
        ELSE NULL
        END AS max_discount,
    -- 标准化活动状态
    CASE
        WHEN o.status NOT IN ('进行中', '已结束', '未开始', '已暂停') THEN '未开始'
        ELSE o.status
        END AS status,
    -- 转换创建时间为TIMESTAMP（关键修复）
    to_timestamp(
        from_unixtime(unix_timestamp(o.create_time, 'yyyy-MM-dd')),
        'yyyy-MM-dd HH:mm:ss'
    ) AS create_time,
    -- 转换更新时间为TIMESTAMP（关键修复）
    to_timestamp(
        CASE
            WHEN unix_timestamp(o.update_time, 'yyyy-MM-dd') < unix_timestamp(o.create_time, 'yyyy-MM-dd')
                THEN from_unixtime(unix_timestamp(o.create_time, 'yyyy-MM-dd'))
            ELSE from_unixtime(unix_timestamp(o.update_time, 'yyyy-MM-dd'))
        END,
        'yyyy-MM-dd HH:mm:ss'
    ) AS update_time
FROM ods_activity_info o
WHERE o.activity_id IS NOT NULL;
    """

    print(f"[INFO] 开始执行SQL查询")
    df1 = spark.sql(select_sql1)

    # 验证字段类型（确认所有时间字段为timestamp）
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
    execute_hive_insert(target_date, 'dwd_activity_info')