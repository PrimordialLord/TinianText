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
    -- 商品ID（以运营表为准，确保非空）
    pos.product_id,

    -- 商品名称（兼容基础表无数据的情况）
    COALESCE(pbd.product_name, '未知商品'),

    -- 分类名称（兼容基础表无数据的情况）
    COALESCE(pbd.category_name, '未知分类'),

    -- 综合评分（兼容评分表无数据的情况）
    COALESCE(dpss.total_score, 0.0),

    -- 评分等级（兼容评分表无数据的情况）
    COALESCE(dpss.score_level, '未评级'),

    -- 价格-销量比（直接取自运营表）
    pos.price_sales_ratio,

    -- 访客-销量比（直接取自运营表）
    pos.visitor_sales_ratio,

    -- 销售金额（直接取自运营表）
    pos.sales_amount,

    -- 销量（直接取自运营表）
    pos.sales_volume,

    -- 统计日期
    '2025-08-03' AS stat_date
FROM
    -- 主表：运营指标汇总表（保留所有数据）
    dws_product_operation_summary pos
-- 左关联评分表（允许无评分数据）
        LEFT JOIN dws_product_score_summary dpss
                  ON pos.product_id = dpss.product_id
                      AND pos.dt = dpss.dt
-- 左关联基础表（允许无基础信息）
        LEFT JOIN dwd_product_base_detail pbd
                  ON pos.product_id = pbd.product_id
                      AND pos.dt = pbd.dt
WHERE pos.dt = '2025-08-03' -- 仅保留日期过滤
  AND pos.product_id IS NOT NULL; 
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
    execute_hive_insert(target_date, 'ads_product_value_assessment')
