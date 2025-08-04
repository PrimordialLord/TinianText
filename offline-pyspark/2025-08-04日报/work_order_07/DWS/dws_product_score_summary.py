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
    -- 商品唯一标识
    pb.product_id,

    -- 流量获取评分
    round(pb.traffic_index, 2)                             AS traffic_score,

    -- 流量转化评分
    round(
            (pb.conversion_rate / 100 * 50) + -- 转化率占比50%
            (pb.conversion_index * 0.5) -- 转化指标占比50%
        , 2)                                               AS conversion_score,

    -- 内容营销评分
    round(pb.content_index, 2)                             AS content_score,

    -- 客户拉新评分
    round(
            (greatest(log10(pb.new_customer_count + 1), 0) / 3 * 40) + -- 新增客户数占比40%
            (pb.new_customer_index * 0.6) -- 拉新指标占比60%
        , 2)                                               AS new_customer_score,

    -- 服务质量评分
    round(pb.service_index, 2)                             AS service_score,

    -- 综合评分（关键修正：用实际表达式替代别名）
    round(
        -- 流量占比20%（使用实际计算逻辑）
            (round(pb.traffic_index, 2) * 0.2) +
                -- 转化占比30%
            (round((pb.conversion_rate / 100 * 50) + (pb.conversion_index * 0.5), 2) * 0.3) +
                -- 内容占比10%
            (round(pb.content_index, 2) * 0.1) +
                -- 拉新占比20%
            (round(
                     (greatest(log10(pb.new_customer_count + 1), 0) / 3 * 40) + (pb.new_customer_index * 0.6), 2
             ) * 0.2) +
                -- 服务占比20%
            (round(pb.service_index, 2) * 0.2)
        , 2)                                               AS total_score,

    -- 评分等级（根据综合评分划分）
    case
        when round(
                     (round(pb.traffic_index, 2) * 0.2) +
                     (round((pb.conversion_rate / 100 * 50) + (pb.conversion_index * 0.5), 2) * 0.3) +
                     (round(pb.content_index, 2) * 0.1) +
                     (round(
                              (greatest(log10(pb.new_customer_count + 1), 0) / 3 * 40) + (pb.new_customer_index * 0.6),
                              2
                      ) * 0.2) +
                     (round(pb.service_index, 2) * 0.2)
                 , 2) >= 90 then 'A'
        when round(
                     (round(pb.traffic_index, 2) * 0.2) +
                     (round((pb.conversion_rate / 100 * 50) + (pb.conversion_index * 0.5), 2) * 0.3) +
                     (round(pb.content_index, 2) * 0.1) +
                     (round(
                              (greatest(log10(pb.new_customer_count + 1), 0) / 3 * 40) + (pb.new_customer_index * 0.6),
                              2
                      ) * 0.2) +
                     (round(pb.service_index, 2) * 0.2)
                 , 2) >= 80 then 'B'
        when round(
                     (round(pb.traffic_index, 2) * 0.2) +
                     (round((pb.conversion_rate / 100 * 50) + (pb.conversion_index * 0.5), 2) * 0.3) +
                     (round(pb.content_index, 2) * 0.1) +
                     (round(
                              (greatest(log10(pb.new_customer_count + 1), 0) / 3 * 40) + (pb.new_customer_index * 0.6),
                              2
                      ) * 0.2) +
                     (round(pb.service_index, 2) * 0.2)
                 , 2) >= 60 then 'C'
        else 'D'
        end                                                AS score_level,

    -- ETL处理时间
    from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss') AS etl_time
FROM dwd_product_base_detail pb_base
         JOIN dwd_product_behavior_detail pb
              ON pb_base.product_id = pb.product_id
                  AND pb_base.dt = pb.dt
WHERE pb_base.dt = '2025-08-03'
  AND pb.traffic_index IS NOT NULL
  AND pb.service_index IS NOT NULL;
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
    execute_hive_insert(target_date, 'dws_product_score_summary')
