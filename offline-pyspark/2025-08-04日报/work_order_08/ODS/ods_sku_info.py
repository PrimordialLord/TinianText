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


def create_ods_sku_info_table(spark):
    """创建ods_sku_info表，确保表结构与DataFrame匹配"""
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS ods_sku_info (
        sku_id BIGINT COMMENT 'SKU唯一ID',
        product_id BIGINT COMMENT '关联商品ID',
        sku_name STRING COMMENT 'SKU名称',
        price DECIMAL(10,2) COMMENT 'SKU价格',
        stock INT COMMENT '库存数量',
        raw_data STRING COMMENT '原始数据JSON'
    )
    STORED AS ORC
    TBLPROPERTIES (
        'orc.compress'='snappy',
        'comment'='SKU信息原始表'
    )
    """
    spark.sql(create_table_sql)
    print("[INFO] 表ods_sku_info创建或已存在")


def select_to_hive(jdbcDF, tableName):
    """将DataFrame数据追加写入Hive表"""
    jdbcDF.write.mode('append').insertInto(f"{tableName}")


def execute_hive_insert(partition_date: str, tableName):
    """从源表查询数据并写入目标Hive分区表"""
    spark = get_spark_session()

    # 关键修复：先创建目标表
    create_ods_sku_info_table(spark)

    # 构建动态SQL查询
    select_sql1 = """
SELECT
    -- SKU ID（10000-11000范围内唯一）
    10000 + row_number() OVER (ORDER BY pos)                 AS sku_id,
    -- 商品ID（关联商品表的1000-2000范围，确保每个商品对应1-3个SKU）
    1000 + cast(row_number() OVER (ORDER BY pos) / 3 AS INT) AS product_id,
    -- SKU名称（基于商品ID和属性生成，如颜色、尺码）
    concat(
            '商品', 1000 + cast(row_number() OVER (ORDER BY pos) / 3 AS INT),
            '-',
            CASE cast(rand() * 5 AS INT) -- 随机颜色
                WHEN 0 THEN '红色'
                WHEN 1 THEN '蓝色'
                WHEN 2 THEN '黑色'
                WHEN 3 THEN '白色'
                ELSE '灰色'
                END,
            '-',
            CASE cast(rand() * 4 AS INT) -- 随机尺码
                WHEN 0 THEN 'S'
                WHEN 1 THEN 'M'
                WHEN 2 THEN 'L'
                ELSE 'XL'
                END
    )                                            AS sku_name,
    -- SKU价格（基于商品价格浮动±10%，保留两位小数）
    cast(
            (100 + rand() * 1900) * (0.9 + rand() * 0.2) -- 100-2000元基础上浮动
        AS DECIMAL(10, 2)
    )                                            AS price,
    -- 库存数量（10-1000的非负整数，符合实际业务场景）
    cast(rand() * 990 + 10 AS INT)               AS stock,
    -- 原始数据JSON串
    concat(
            '{"sku_id":', 10000 + row_number() OVER (ORDER BY pos),
            ',"product_id":', 1000 + cast(row_number() OVER (ORDER BY pos) / 3 AS INT),
            ',"sku_name":"', concat(
                    '商品', 1000 + cast(row_number() OVER (ORDER BY pos) / 3 AS INT),
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
                        ELSE 'XL'
                        END
                             ),
            ',"price":', cast((100 + rand() * 1900) * (0.9 + rand() * 0.2) AS DECIMAL(10, 2)),
            ',"stock":', cast(rand() * 990 + 10 AS INT),
            '}'
    )                                            AS raw_data
FROM (
         -- 生成1000条数据的基础序列
         SELECT pos
         FROM (SELECT posexplode(split(space(999), '')) AS (pos, val)) t) temp;
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
    execute_hive_insert(target_date, 'ods_sku_info')