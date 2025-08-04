from pyspark.sql import SparkSession
from pyspark.sql.functions import lit


def get_spark_session():
    """初始化SparkSession并配置Hive连接，确保数据库存在"""
    spark = SparkSession.builder \
        .appName("HiveETL") \
        .config("spark.hadoop.hive.metastore.uris", "thrift://cdh01:9083") \
        .config("spark.sql.hive.convertMetastoreOrc", "true") \
        .config("spark.sql.orc.compression.codec", "snappy") \
        .config("hive.exec.dynamic.partition.mode", "nonstrict") \
        .config("spark.sql.hive.ignoreMissingPartitions", "true") \
        .enableHiveSupport() \
        .getOrCreate()

    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    spark.sql("CREATE DATABASE IF NOT EXISTS work_order_08")
    spark.sql("USE work_order_08")

    return spark


def create_dwd_sku_info_table(spark):
    """创建目标表（若不存在），确保表结构匹配"""
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS dwd_sku_info (
        sku_id BIGINT COMMENT 'SKU唯一ID',
        product_id BIGINT COMMENT '关联商品ID',
        sku_name STRING COMMENT 'SKU名称',
        price DECIMAL(10,2) COMMENT 'SKU价格',
        stock INT COMMENT '库存数量'
    )
    STORED AS ORC
    TBLPROPERTIES (
        'orc.compress'='snappy',
        'comment'='SKU信息明细表'
    )
    """
    spark.sql(create_table_sql)
    print("[INFO] 表dwd_sku_info创建或已存在")


def select_to_hive(jdbcDF, tableName):
    """将DataFrame数据追加写入Hive表"""
    jdbcDF.write.mode('append').insertInto(f"{tableName}")


def execute_hive_insert(partition_date: str, tableName):
    """从源表查询数据并写入目标Hive分区表"""
    spark = get_spark_session()

    # 先创建目标表（关键修复：避免表不存在错误）
    create_dwd_sku_info_table(spark)

    # 构建动态SQL查询，修复row_number()排序问题
    select_sql1 = """
SELECT
    -- 统计周期（日/7天/30天）
    stats.period                                                                                                     AS statistics_period,
    -- 统计开始日期
    stats.start_date,
    -- 统计结束日期
    stats.end_date,
    -- 总发送次数：兼容空数据（无数据时为0）
    COALESCE(SUM(effect.send_count), 0)                                                                              AS total_send_count,
    -- 总支付次数：兼容空数据
    COALESCE(SUM(effect.pay_count), 0)                                                                               AS total_pay_count,
    -- 总支付金额：兼容空数据
    COALESCE(SUM(effect.total_pay_amount), 0.00)                                                                     AS total_pay_amount,
    -- 平均优惠金额：处理除数为0和空值
    CASE
        WHEN COALESCE(SUM(effect.pay_count), 0) = 0 THEN 0.00
        ELSE ROUND(COALESCE(SUM(effect.total_pay_amount), 0.00) / SUM(effect.pay_count), 2)
        END                                                                                                          AS avg_discount,
    -- 进行中活动数：直接从活动表统计，不依赖效果表
    COUNT(DISTINCT CASE
                       WHEN act.status = '进行中' AND
                            DATE(act.start_time) <= stats.end_date AND
                            DATE(act.end_time) >= stats.start_date
                           THEN act.activity_id END)                                                                 AS activity_in_progress_count,
    -- 已结束活动数：直接从活动表统计
    COUNT(DISTINCT CASE
                       WHEN act.status = '已结束' AND
                            DATE(act.end_time) BETWEEN stats.start_date AND stats.end_date
                           THEN act.activity_id END)                                                                 AS activity_ended_count,
    -- 数据创建时间
    CURRENT_TIMESTAMP()                                                                                              AS create_time
FROM
    -- 生成独立的统计周期（即使无效果数据也会保留周期记录）
    (
        -- 1. 日周期：近30天的每一天
        SELECT '日'                                 AS period,
               DATE(DATE_ADD(CURRENT_DATE(), -day)) AS start_date,
               DATE(DATE_ADD(CURRENT_DATE(), -day)) AS end_date
        FROM (SELECT pos AS day
              FROM (SELECT posexplode(split(space(29), '')) AS (pos, val)) t) num
        UNION ALL
        -- 2. 7天周期：近7天内的连续7天范围
        SELECT '7天'                                    AS period,
               DATE(DATE_ADD(CURRENT_DATE(), -day - 6)) AS start_date,
               DATE(DATE_ADD(CURRENT_DATE(), -day))     AS end_date
        FROM (SELECT pos AS day
              FROM (SELECT posexplode(split(space(6), '')) AS (pos, val)) t) num
        UNION ALL
        -- 3. 30天周期：固定最近30天
        SELECT '30天'                              AS period,
               DATE(DATE_ADD(CURRENT_DATE(), -29)) AS start_date,
               CURRENT_DATE()                      AS end_date) stats
-- 左关联活动效果表（允许无效果数据的周期保留）
        LEFT JOIN work_order_08.dws_activity_effect_summary effect
                  ON effect.statistics_period = stats.period
                      AND effect.start_date = stats.start_date
                      AND effect.end_date = stats.end_date
-- 左关联活动表（跨库关联，确保活动数据可访问）
        LEFT JOIN work_order_08.dwd_activity_info act ON 1 = 1
-- 按统计周期分组（确保每个周期都有记录）
GROUP BY stats.period,
         stats.start_date,
         stats.end_date;
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
    execute_hive_insert(target_date, 'ads_tool_effect_overview')