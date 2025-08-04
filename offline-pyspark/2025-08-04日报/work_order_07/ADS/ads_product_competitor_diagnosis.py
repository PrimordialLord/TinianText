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
SELECT cd.product_id,
       COALESCE(pb.product_name, '未知商品')                               AS product_name,
       cd.competitor_product_id,
       cd.dimension,
       cd.self_value,
       -- 同维度竞品平均值
       round(AVG(cd.competitor_value) OVER (PARTITION BY cd.dimension), 2) AS competitor_avg_value,
       -- 差距比例（保留表达式用于后续判断）
       round(
               CASE
                   WHEN AVG(cd.competitor_value) OVER (PARTITION BY cd.dimension) > 0
                       THEN (AVG(cd.competitor_value) OVER (PARTITION BY cd.dimension) - cd.self_value)
                                / AVG(cd.competitor_value) OVER (PARTITION BY cd.dimension) * 100
                   ELSE 0
                   END, 2
       )                                                                   AS gap_ratio,
       -- 优化建议（用实际计算表达式替代gap_ratio别名）
       CASE
           WHEN round(
                        CASE
                            WHEN AVG(cd.competitor_value) OVER (PARTITION BY cd.dimension) > 0
                                THEN (AVG(cd.competitor_value) OVER (PARTITION BY cd.dimension) - cd.self_value)
                                         / AVG(cd.competitor_value) OVER (PARTITION BY cd.dimension) * 100
                            ELSE 0
                            END, 2
                ) >= 30 THEN CONCAT('【紧急优化】本品在"', cd.dimension, '"维度落后竞品30%以上，需优先改进')

           WHEN round(
                        CASE
                            WHEN AVG(cd.competitor_value) OVER (PARTITION BY cd.dimension) > 0
                                THEN (AVG(cd.competitor_value) OVER (PARTITION BY cd.dimension) - cd.self_value)
                                         / AVG(cd.competitor_value) OVER (PARTITION BY cd.dimension) * 100
                            ELSE 0
                            END, 2
                ) >= 10 THEN CONCAT('【重点优化】本品在"', cd.dimension, '"维度落后竞品10%-30%，建议针对性提升')

           WHEN round(
                        CASE
                            WHEN AVG(cd.competitor_value) OVER (PARTITION BY cd.dimension) > 0
                                THEN (AVG(cd.competitor_value) OVER (PARTITION BY cd.dimension) - cd.self_value)
                                         / AVG(cd.competitor_value) OVER (PARTITION BY cd.dimension) * 100
                            ELSE 0
                            END, 2
                ) > 0 THEN CONCAT('【轻微优化】本品在"', cd.dimension, '"维度落后竞品0-10%，可微调优化')

           WHEN round(
                        CASE
                            WHEN AVG(cd.competitor_value) OVER (PARTITION BY cd.dimension) > 0
                                THEN (AVG(cd.competitor_value) OVER (PARTITION BY cd.dimension) - cd.self_value)
                                         / AVG(cd.competitor_value) OVER (PARTITION BY cd.dimension) * 100
                            ELSE 0
                            END, 2
                ) <= 0 THEN CONCAT('【保持优势】本品在"', cd.dimension, '"维度优于竞品平均水平，建议维持')

           ELSE '【数据异常】无法计算差距，需检查原始数据'
           END                                                             AS optimization_suggestion,
       '2025-08-03'                                                        AS stat_date
FROM dwd_competitor_detail cd
         LEFT JOIN dwd_product_base_detail pb
                   ON cd.product_id = pb.product_id
                       AND cd.dt = pb.dt
WHERE cd.dt = '2025-08-03'
  AND cd.self_value IS NOT NULL
  AND cd.competitor_value IS NOT NULL;
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
    execute_hive_insert(target_date, 'ads_product_competitor_diagnosis')
