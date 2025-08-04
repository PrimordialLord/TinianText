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

    # 构建动态SQL查询，修复row_number()需要ORDER BY的问题
    select_sql1 = """
SELECT
    -- 生成唯一关联记录ID，添加ORDER BY pos确保排序
    row_number() OVER (ORDER BY pos)           AS id,
    -- 活动ID（关联1-100的活动，符合每个活动最多500个商品的规则）
    cast(rand() * 100 + 1 AS BIGINT)          AS activity_id,
    -- 商品ID（1000-2000范围内随机生成）
    cast(rand() * 1000 + 1000 AS BIGINT)      AS product_id,
    -- SKU ID（商品级活动为NULL，SKU级活动生成10000-15000范围的ID）
    CASE
        WHEN cast(rand() * 2 AS INT) = 0 THEN NULL -- 商品级活动
        ELSE cast(rand() * 5000 + 10000 AS BIGINT) -- SKU级活动
        END                                   AS sku_id,
    -- 优惠金额（1-5000的整数，符合文档中金额为整数且不超过5000元的规则）
    cast(rand() * 5000 + 1 AS DECIMAL(10, 2)) AS discount_amount,
    -- 限购次数（1-5次，默认1次可修改）
    cast(rand() * 4 + 1 AS INT)               AS limit_purchase,
    -- 是否发布（0未发布/1已发布，随机生成）
    cast(rand() * 2 AS TINYINT)               AS is_published,
    -- 添加时间（活动开始时间后1-30天）
    date_add(
            date_add('2025-01-01', cast(rand() * 60 AS INT)),
            cast(rand() * 30 + 1 AS INT)
    )                                         AS add_time,
    -- 移出时间（已发布商品不可移出，未发布商品可能有移出时间）
    CASE
        WHEN cast(rand() * 2 AS TINYINT) = 0 THEN NULL -- 未移出
        WHEN cast(rand() * 2 AS TINYINT) = 1 AND cast(rand() * 2 AS TINYINT) = 0
            THEN date_add(current_date, -cast(rand() * 10 + 1 AS INT)) -- 未发布商品的移出时间
        ELSE NULL -- 已发布商品不可移出
        END                                   AS remove_time,
    -- 原始数据JSON串，使用row_number()时同样需要ORDER BY
    concat(
            '{"id":', row_number() OVER (ORDER BY pos),
            ',"activity_id":', cast(rand() * 100 + 1 AS BIGINT),
            ',"product_id":', cast(rand() * 1000 + 1000 AS BIGINT),
            ',"sku_id":', CASE
                              WHEN cast(rand() * 2 AS INT) = 0 THEN 'null'
                              ELSE cast(rand() * 5000 + 10000 AS BIGINT)
                END,
            ',"discount_amount":', cast(rand() * 5000 + 1 AS INT),
            ',"limit_purchase":', cast(rand() * 4 + 1 AS INT),
            ',"is_published":', cast(rand() * 2 AS TINYINT),
            '}'
    )                                         AS raw_data
-- 生成1000条数据
FROM (SELECT pos
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
    execute_hive_insert(target_date, 'ods_product_activity_rel')
