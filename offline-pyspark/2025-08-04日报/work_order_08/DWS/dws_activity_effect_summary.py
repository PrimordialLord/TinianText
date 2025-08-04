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
    -- 活动编号
    act.activity_id,
    -- 活动名称
    act.activity_name,
    -- 统计周期（通过条件判断当前汇总的周期类型）
    stats.period                                                       AS statistics_period,
    -- 统计开始日期
    stats.start_date,
    -- 统计结束日期
    stats.end_date,
    -- 发送次数：该周期内客服发送的优惠次数
    COUNT(DISTINCT send.id)                                            AS send_count,
    -- 支付次数：该周期内使用优惠完成支付的次数（假设use_time对应支付时间）
    COUNT(DISTINCT CASE WHEN send.is_used = 1 THEN send.id END)        AS pay_count,
    -- 总支付金额：需关联订单表，此处用“优惠金额*使用次数”模拟（实际应关联订单金额）
    SUM(CASE WHEN send.is_used = 1 THEN send.send_discount ELSE 0 END) AS total_pay_amount,
    -- 优惠使用率：使用次数/发送次数（避免除数为0）
    CASE
        WHEN COUNT(DISTINCT send.id) = 0 THEN 0
        ELSE ROUND(
                COUNT(DISTINCT CASE WHEN send.is_used = 1 THEN send.id END)
                    / COUNT(DISTINCT send.id) * 100,
                2
             )
        END                                                            AS use_rate,
    -- 数据创建时间（当前时间）
    CURRENT_TIMESTAMP()                                                AS create_time
FROM
    -- 主表：活动明细事实表（获取活动基础信息）
    dwd_activity_info act
-- 关联统计周期维度（通过子查询生成周期范围）
        CROSS JOIN (
        -- 生成日/7天/30天三种周期的统计范围（以活动结束时间为基准，实际可按业务需求调整）
        SELECT '日'               AS period,
               DATE(act.end_time) AS start_date,
               DATE(act.end_time) AS end_date
        FROM dwd_activity_info act
        UNION ALL
        SELECT '7天'                            AS period,
               DATE(DATE_ADD(act.end_time, -6)) AS start_date, -- 结束时间前6天（共7天）
               DATE(act.end_time)               AS end_date
        FROM dwd_activity_info act
        UNION ALL
        SELECT '30天'                            AS period,
               DATE(DATE_ADD(act.end_time, -29)) AS start_date, -- 结束时间前29天（共30天）
               DATE(act.end_time)                AS end_date
        FROM dwd_activity_info act) stats
-- 关联客服发送优惠明细（获取发送和使用数据）
        LEFT JOIN dwd_customer_service_send_detail send
                  ON act.activity_id = send.activity_id
                      AND DATE(send.send_time) BETWEEN stats.start_date AND stats.end_date -- 发送时间在统计周期内
-- 过滤有效活动
WHERE act.activity_id IS NOT NULL
-- 按活动和统计周期分组
GROUP BY act.activity_id,
         act.activity_name,
         stats.period,
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
    execute_hive_insert(target_date, 'dws_activity_effect_summary')