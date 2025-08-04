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
    -- 活动基础信息
    act.activity_id,
    act.activity_name,
    act.activity_level,
    act.activity_type,
    act.start_time,
    act.end_time,
    -- 活动效果指标
    COALESCE(effect.total_send, 0)      AS send_count,
    COALESCE(effect.total_pay, 0)       AS pay_count,
    COALESCE(effect.total_amount, 0.00) AS pay_amount,
    -- 使用率计算
    CASE
        WHEN COALESCE(effect.total_send, 0) = 0 THEN 0.00
        ELSE ROUND(effect.total_pay / effect.total_send * 100, 2)
        END                             AS use_rate,
    -- 发送次数前三的商品ID（确保array<bigint>类型一致）
    CASE
        WHEN product_rank.top3_products IS NULL THEN array(cast(NULL AS BIGINT)) -- 显式bigint空数组
        ELSE product_rank.top3_products
        END                             AS top3_product_id,
    -- 数据创建时间
    CURRENT_TIMESTAMP()                 AS create_time
FROM work_order_08.dwd_activity_info act
         LEFT JOIN (SELECT activity_id,
                           SUM(send_count)       AS total_send,
                           SUM(pay_count)        AS total_pay,
                           SUM(total_pay_amount) AS total_amount
                    FROM work_order_08.dws_activity_effect_summary
                    GROUP BY activity_id) effect ON act.activity_id = effect.activity_id
         LEFT JOIN (SELECT activity_id,
                           -- 强制生成array<bigint>
                           collect_list(cast(t.product_id AS BIGINT)) AS top3_products
                    FROM (SELECT send.activity_id,
                                 send.product_id,
                                 COUNT(DISTINCT send.id) AS product_send_count,
                                 ROW_NUMBER() OVER (
                                     PARTITION BY send.activity_id
                                     ORDER BY COUNT(DISTINCT send.id) DESC
                                     )                   AS rnk
                          FROM work_order_08.dwd_customer_service_send_detail send
                          WHERE send.product_id IS NOT NULL
                          GROUP BY send.activity_id, send.product_id) t
                    WHERE rnk <= 3
                    GROUP BY activity_id) product_rank ON act.activity_id = product_rank.activity_id;
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
    execute_hive_insert(target_date, 'ads_activity_detail')