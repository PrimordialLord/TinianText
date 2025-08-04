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
    -- 活动编号（可能为NULL，代表非活动商品）
    act.activity_id,
    -- 活动名称（非活动商品对应NULL）
    act.activity_name,
    -- 发送次数：该商品在活动中的总发送次数
    COALESCE(SUM(send.total_send), 0) AS send_count,
    -- 支付次数：该商品在活动中被使用的支付次数
    COALESCE(SUM(send.total_pay), 0)  AS pay_count,
    -- 平均优惠金额：使用优惠的平均金额
    CASE
        WHEN COALESCE(SUM(send.total_pay), 0) = 0 THEN 0.00
        ELSE ROUND(SUM(send.total_discount) / SUM(send.total_pay), 2)
        END                           AS discount_amount_avg,
    -- 数据创建时间
    CURRENT_TIMESTAMP()               AS create_time
FROM
    -- 主表：商品明细信息
    work_order_08.dwd_product_info p
-- 关联商品活动关联表（获取商品参与的活动）
        LEFT JOIN work_order_08.dwd_product_activity_rel rel
                  ON p.product_id = rel.product_id
-- 关联活动信息表（获取活动名称）
        LEFT JOIN work_order_08.dwd_activity_info act
                  ON rel.activity_id = act.activity_id
-- 关联商品优惠发送汇总数据（按商品+活动分组）
        LEFT JOIN (SELECT product_id,
                          activity_id,
                          -- 该商品在活动中的总发送次数
                          COUNT(DISTINCT id)                                       AS total_send,
                          -- 该商品在活动中的总支付次数
                          COUNT(DISTINCT CASE WHEN is_used = 1 THEN id END)        AS total_pay,
                          -- 该商品在活动中被使用的优惠总金额
                          SUM(CASE WHEN is_used = 1 THEN send_discount ELSE 0 END) AS total_discount
                   FROM work_order_08.dwd_customer_service_send_detail
                   GROUP BY product_id, activity_id) send
                  ON p.product_id = send.product_id
                      AND rel.activity_id = send.activity_id
-- 按商品和活动分组（确保每个商品在每个活动中生成一条记录）
GROUP BY p.product_id,
         p.product_name,
         act.activity_id,
         act.activity_name;
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
    execute_hive_insert(target_date, 'ads_product_effect')