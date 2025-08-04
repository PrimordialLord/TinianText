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
    -- 生成SKU ID（10000开始的自增ID），添加ORDER BY确保排序
    10000 + row_number() OVER (ORDER BY p.product_id, t.pos)  AS sku_id,
    -- 商品ID（关联商品表确保存在）
    p.product_id,
    -- 生成规范的SKU名称
    CONCAT(
            p.product_name,
            '-',
            CASE cast(rand() * 5 AS INT)
                WHEN 0 THEN '红色'
                WHEN 1 THEN '蓝色'
                WHEN 2 THEN '黑色'
                WHEN 3 THEN '白色'
                ELSE '灰色'
                END,
            '-',
            CASE cast(rand() * 4 AS INT)
                WHEN 0 THEN 'S'
                WHEN 1 THEN 'M'
                WHEN 2 THEN 'L'
                ELSE 'XL'  -- 修复原代码中重复的WHEN 2条件
                END
    )                                                      AS sku_name,
    -- 基于商品价格生成合理的SKU价格
    cast(p.price * (0.9 + rand() * 0.6) AS DECIMAL(10, 2)) AS price,
    -- 生成合理的库存数量
    cast(rand() * 990 + 10 AS INT)                         AS stock
-- 从商品表获取基础数据，为每个商品生成1-3个SKU
FROM dwd_product_info p
-- 生成每个商品对应3个SKU的序列（通过posexplode生成0、1、2三个位置）
         LATERAL VIEW posexplode(split(space(2), '')) t AS pos, val
-- 确保商品ID有效
WHERE p.product_id IS NOT NULL;
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
    execute_hive_insert(target_date, 'dwd_sku_info')