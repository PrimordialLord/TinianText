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
    -- 商品ID（1000-2000范围内唯一）
    1000 + row_number() OVER (ORDER BY pos)            AS product_id,
    -- 商品名称（随机生成，不超过50字）
    concat(
            CASE cast(rand() * 5 AS INT)
                WHEN 0 THEN '男士'
                WHEN 1 THEN '女士'
                WHEN 2 THEN '儿童'
                WHEN 3 THEN '户外'
                ELSE '家用'
                END,
            CASE cast(rand() * 8 AS INT)
                WHEN 0 THEN 'T恤'
                WHEN 1 THEN '裤子'
                WHEN 2 THEN '鞋子'
                WHEN 3 THEN '背包'
                WHEN 4 THEN '手表'
                WHEN 5 THEN '耳机'
                WHEN 6 THEN '水杯'
                ELSE '帽子'
                END,
            '-',
            cast(row_number() OVER (ORDER BY pos) AS STRING)
    )                                                  AS product_name,
    -- 商品价格（10-2000元随机，保留两位小数）
    cast(rand() * 1990 + 10 AS DECIMAL(10, 2))         AS price,
    -- 红线价（默认是商品标价的四折，符合文档规则）
    cast((rand() * 1990 + 10) * 0.4 AS DECIMAL(10, 2)) AS red_line_price,
    -- 创建时间（近1年内随机生成）
    date_add(current_date, -cast(rand() * 365 AS INT)) AS create_time,
    -- 更新时间（创建时间之后，随机生成）
    date_add(
            date_add(current_date, -cast(rand() * 365 AS INT)),
            cast(rand() * 100 AS INT)
    )                                                  AS update_time,
    -- 原始数据JSON串
    concat(
            '{"product_id":', 1000 + row_number() OVER (ORDER BY pos),
            ',"product_name":"', concat(
                    CASE cast(rand() * 5 AS INT)
                        WHEN 0 THEN '男士'
                        WHEN 1 THEN '女士'
                        WHEN 2 THEN '儿童'
                        WHEN 3 THEN '户外'
                        ELSE '家用'
                        END,
                    CASE cast(rand() * 8 AS INT)
                        WHEN 0 THEN 'T恤'
                        WHEN 1 THEN '裤子'
                        WHEN 2 THEN '鞋子'
                        WHEN 3 THEN '背包'
                        WHEN 4 THEN '手表'
                        WHEN 5 THEN '耳机'
                        WHEN 6 THEN '水杯'
                        ELSE '帽子'
                        END,
                    '-',
                    cast(row_number() OVER (ORDER BY pos) AS STRING)
                                 ),
            ',"price":', cast(rand() * 1990 + 10 AS DECIMAL(10, 2)),
            ',"red_line_price":', cast((rand() * 1990 + 10) * 0.4 AS DECIMAL(10, 2)),
            '}'
    )                                                  AS raw_data
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
    execute_hive_insert(target_date, 'ods_product_info')
