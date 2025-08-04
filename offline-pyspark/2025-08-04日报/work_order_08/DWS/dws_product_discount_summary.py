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
    -- 商品ID
    p.product_id,
    -- 商品名称
    p.product_name,
    -- 统计周期（日/7天/30天）
    stats.period                                                AS statistics_period,
    -- 统计开始日期
    stats.start_date,
    -- 统计结束日期
    stats.end_date,
    -- 参与活动数：周期内商品参与的 distinct 活动数量
    COUNT(DISTINCT rel.activity_id)                             AS activity_count,
    -- 总发送次数：周期内该商品相关优惠的发送总次数
    COUNT(DISTINCT send.id)                                     AS total_send_count,
    -- 总支付次数：周期内该商品优惠被使用的支付次数
    COUNT(DISTINCT CASE WHEN send.is_used = 1 THEN send.id END) AS total_pay_count,
    -- 数据创建时间
    CURRENT_TIMESTAMP()                                         AS create_time
FROM
    -- 主表：商品明细事实表
    dwd_product_info p
-- 关联商品活动关联表（获取商品参与的活动）
        LEFT JOIN dwd_product_activity_rel rel
                  ON p.product_id = rel.product_id
-- 关联客服发送优惠明细（获取优惠发送和使用数据）
        LEFT JOIN dwd_customer_service_send_detail send
                  ON p.product_id = send.product_id
                      AND (rel.activity_id = send.activity_id OR rel.activity_id IS NULL) -- 关联同一活动或无活动记录
-- 关联统计周期维度（兼容低版本Hive）
        CROSS JOIN (
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
-- 过滤条件：活动关联时间或发送时间在统计周期内
WHERE (rel.add_time IS NULL OR DATE(rel.add_time) BETWEEN stats.start_date AND stats.end_date)
  AND (send.send_time IS NULL OR DATE(send.send_time) BETWEEN stats.start_date AND stats.end_date)
-- 按商品和统计周期分组
GROUP BY p.product_id,
         p.product_name,
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
    execute_hive_insert(target_date, 'dws_product_discount_summary')