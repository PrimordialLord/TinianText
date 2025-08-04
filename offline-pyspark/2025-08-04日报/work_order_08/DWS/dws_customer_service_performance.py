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
    -- 客服ID
    send.customer_service_id,
    -- 客服姓名（用ID拼接默认名称）
    CONCAT('客服_', send.customer_service_id)                          AS customer_service_name,
    -- 统计周期（日/7天/30天）
    stats.period                                                       AS statistics_period,
    -- 统计开始日期
    stats.start_date,
    -- 统计结束日期
    stats.end_date,
    -- 发送次数：周期内客服发送的优惠总次数
    COUNT(DISTINCT send.id)                                            AS send_count,
    -- 成功支付次数：周期内发送的优惠被使用并完成支付的次数
    COUNT(DISTINCT CASE WHEN send.is_used = 1 THEN send.id END)        AS successful_pay_count,
    -- 贡献金额：使用优惠产生的支付金额
    SUM(CASE WHEN send.is_used = 1 THEN send.send_discount ELSE 0 END) AS contribution_amount,
    -- 数据创建时间（当前时间）
    CURRENT_TIMESTAMP()                                                AS create_time
FROM
    -- 主表：客服发送优惠明细事实表
    dwd_customer_service_send_detail send
-- 关联统计周期维度（兼容低版本Hive的周期生成方式）
        CROSS JOIN (
        -- 1. 日周期：近30天的每一天
        SELECT '日'                                 AS period,
               DATE(DATE_ADD(CURRENT_DATE(), -day)) AS start_date,
               DATE(DATE_ADD(CURRENT_DATE(), -day)) AS end_date
        FROM (
                 -- 生成0-29的数字序列（代表近30天）
                 SELECT pos AS day
                 FROM (SELECT posexplode(split(space(29), '')) AS (pos, val)) t) num
        UNION ALL
        -- 2. 7天周期：近7天内，以每天为结束日的连续7天
        SELECT '7天'                                    AS period,
               DATE(DATE_ADD(CURRENT_DATE(), -day - 6)) AS start_date, -- 开始日期=结束日期-6天
               DATE(DATE_ADD(CURRENT_DATE(), -day))     AS end_date
        FROM (
                 -- 生成0-6的数字序列（代表近7天）
                 SELECT pos AS day
                 FROM (SELECT posexplode(split(space(6), '')) AS (pos, val)) t) num
        UNION ALL
        -- 3. 30天周期：固定为最近30天
        SELECT '30天'                              AS period,
               DATE(DATE_ADD(CURRENT_DATE(), -29)) AS start_date, -- 30天前
               CURRENT_DATE()                      AS end_date) stats
-- 过滤条件：发送时间在统计周期内
WHERE DATE(send.send_time) BETWEEN stats.start_date AND stats.end_date
-- 按客服ID和统计周期分组
GROUP BY send.customer_service_id,
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
    execute_hive_insert(target_date, 'dws_customer_service_performance')