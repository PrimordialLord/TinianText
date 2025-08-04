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

    spark.sql("CREATE DATABASE IF NOT EXISTS work_order_07")
    spark.sql("USE work_order_07")

    return spark


def select_to_hive(jdbcDF, tableName):
    """将DataFrame数据追加写入Hive表"""
    jdbcDF.write.mode('append').insertInto(f"{tableName}")


def execute_hive_insert(partition_date: str, tableName):
    """从源表查询数据并写入目标Hive分区表"""
    spark = get_spark_session()

    # 构建动态SQL查询，修复了字符串格式化问题
    select_sql1 = """
SELECT
    -- 商品唯一标识（直接复用ODS层清洗后的ID）
    opb.product_id,

    -- 商品名称（保留原始名称，已在ODS层完成格式化）
    opb.product_name,

    -- 商品分类ID（关联分类体系）
    opb.category_id,

    -- 商品分类名称（冗余存储，便于分析）
    opb.category_name,

    -- 品牌ID（关联品牌体系）
    opb.brand_id,

    -- 品牌名称（冗余存储，提升查询效率）
    opb.brand_name,

    -- 商品售价（保留两位小数，确保精度）
    round(opb.price, 2)                                    AS price,

    -- ETL处理时间（记录当前转换时间）
    from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss') AS etl_time
FROM
    -- 源表：ODS层商品基础信息表（指定分区）
    ods_product_base opb
WHERE
  -- 过滤当前分区数据
    opb.dt = '2025-08-03'
  -- 数据清洗：过滤无效商品ID（确保主键非空）
  AND opb.product_id IS NOT NULL
  AND opb.product_id != ''
  -- 清洗异常价格（排除0或负数价格）
  AND opb.price > 0;
    """

    print(f"[INFO] 开始执行SQL查询，目标分区：{partition_date}")
    df1 = spark.sql(select_sql1)

    # 添加分区字段ds
    df_with_partition = df1.withColumn("dt", lit(partition_date))

    print(f"[INFO] SQL执行完成，分区{partition_date}操作成功")
    df_with_partition.show(5)

    # 写入数据
    select_to_hive(df_with_partition, tableName)

    # 验证数据
    print(f"[INFO] 验证分区 {partition_date} 的数据...")
    verify_df = spark.sql(f"SELECT * FROM {tableName} WHERE dt='{partition_date}' LIMIT 5")
    verify_df.show()


if __name__ == "__main__":
    target_date = '2025-08-03'
    execute_hive_insert(target_date, 'dwd_product_base_detail')
