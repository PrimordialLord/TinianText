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
    -- 商品ID（与商品基础表关联逻辑，随机生成prod_前缀的8位ID）
    concat('prod_', lower(substr(md5(cast(rand() as string)), 1, 8))) AS product_id,

    -- 访客数（10-10000随机整数）
    cast(floor(10 + rand() * 9990) as bigint)                         AS visitor_count,

    -- 销售金额（0-100000随机数值，保留两位小数）
    round(rand() * 100000, 2)                                         AS sales_amount,

    -- 销量（0-1000随机整数，与销售金额正相关）
    cast(floor(
            case
                when rand() > 0.3 then rand() * 1000 -- 70%概率有销量
                else 0 -- 30%概率无销量
                end
         ) as bigint)                                                 AS sales_volume,

    -- 转化率（0-50%，保留4位小数）
    round(
            case
                when floor(10 + rand() * 9990) > 0 -- 避免除数为0
                    then (floor(rand() * 1000) / floor(10 + rand() * 9990)) * 100
                else 0
                end, 4
    )                                                                 AS conversion_rate,

    -- 新增客户数（0-500随机整数）
    cast(floor(rand() * 500) as bigint)                               AS new_customer_count,

    -- 流量指标原始值（0-100随机数值）
    round(rand() * 100, 2)                                            AS traffic_index,

    -- 转化指标原始值（0-100随机数值）
    round(rand() * 100, 2)                                            AS conversion_index,

    -- 内容指标原始值（0-100随机数值）
    round(rand() * 100, 2)                                            AS content_index,

    -- 拉新指标原始值（0-100随机数值）
    round(rand() * 100, 2)                                            AS new_customer_index,

    -- 服务指标原始值（0-100随机数值）
    round(rand() * 100, 2)                                            AS service_index,

    -- 原始JSON数据（包含所有字段的原始信息）
    concat(
            '{"product_id":"', concat('prod_', lower(substr(md5(cast(rand() as string)), 1, 8))), '",',
            '"visitor_count":', cast(floor(10 + rand() * 9990) as string), ',',
            '"sales_amount":', round(rand() * 100000, 2), ',',
            '"sales_volume":', cast(floor(rand() * 1000) as string), ',',
            '"conversion_rate":', round(rand() * 50, 4), ',',
            '"new_customer_count":', cast(floor(rand() * 500) as string), ',',
            '"metrics":{',
            '"traffic":', round(rand() * 100, 2), ',',
            '"conversion":', round(rand() * 100, 2), ',',
            '"content":', round(rand() * 100, 2), ',',
            '"new_customer":', round(rand() * 100, 2), ',',
            '"service":', round(rand() * 100, 2), ''
                '}},',
            '"collect_time":"', current_date(), ' ',
            lpad(floor(rand() * 24), 2, '0'), ':',
            lpad(floor(rand() * 60), 2, '0'), ':',
            lpad(floor(rand() * 60), 2, '0'), '"'
    )                                                                 AS raw_data,

    -- ETL处理时间（当前时间）
    from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss')            AS etl_time
FROM
    -- 基础表生成100条数据（可调整space参数控制数量）
    (SELECT posexplode(split(space(99), '')) AS (id, val)) nums;
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
    execute_hive_insert(target_date, 'ods_product_behavior')
