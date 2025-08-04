from pyspark.sql import SparkSession
from pyspark.sql.functions import lit  # 无需额外导入to_timestamp，SQL中直接使用

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

    # 构建动态SQL查询，关键：用to_timestamp转换时间为Timestamp类型
    select_sql1 = """
SELECT id                                                         AS activity_id,
       -- 生成不超过10字的活动名称（符合文档中活动名称长度限制）
       CASE
           WHEN id % 8 = 0 THEN '新品特惠'
           WHEN id % 8 = 1 THEN '老客专享'
           WHEN id % 8 = 2 THEN '限时折扣'
           WHEN id % 8 = 3 THEN '满减活动'
           WHEN id % 8 = 4 THEN '会员回馈'
           WHEN id % 8 = 5 THEN '清仓处理'
           WHEN id % 8 = 6 THEN '周末特卖'
           ELSE '节日优惠'
           END                                                    AS activity_name,
       -- 随机生成活动级别（商品级/SKU级，与文档中活动级别分类一致）
       CASE WHEN id % 2 = 0 THEN '商品级' ELSE 'SKU级' END        AS activity_level,
       -- 随机生成优惠类型（固定优惠/自定义优惠，参考文档中优惠类型划分）
       CASE WHEN id % 3 = 0 THEN '固定优惠' ELSE '自定义优惠' END AS activity_type,
       -- 生成活动开始时间（2025年1月-3月）
       date_add('2025-01-01', cast(rand() * 60 AS INT))           AS start_time,
       -- 活动结束时间（开始时间后2-120天，符合文档中活动时间范围规则{insert\_element\_0\_}）
       date_add(
               date_add('2025-01-01', cast(rand() * 60 AS INT)),
               cast(rand() * 119 + 2 AS INT)
       )                                                          AS end_time,
       -- 自定义优惠时生成最大优惠金额（不超过5000元，遵循文档中金额上限要求{insert\_element\_1\_}）
       CASE
           WHEN id % 3 != 0 THEN cast(rand() * 5000 AS DECIMAL(10, 2))
           ELSE NULL
           END                                                    AS max_discount,
       -- 生成活动状态（进行中/已结束/未开始/已暂停，覆盖文档中活动状态类型）
       CASE
           WHEN date_add('2025-01-01', cast(rand() * 60 AS INT)) > current_date THEN '未开始'
           WHEN date_add(
                        date_add('2025-01-01', cast(rand() * 60 AS INT)),
                        cast(rand() * 119 + 2 AS INT)
                ) < current_date THEN '已结束'
           WHEN id % 5 = 0 THEN '已暂停'
           ELSE '进行中'
           END                                                    AS status,
       -- 生成创建时间（活动开始前）
       date_add(
               date_add('2025-01-01', cast(rand() * 60 AS INT)),
               -cast(rand() * 30 + 1 AS INT)
       )                                                          AS create_time,
       -- 生成更新时间（创建时间后）
       date_add(
               date_add(
                       date_add('2025-01-01', cast(rand() * 60 AS INT)),
                       -cast(rand() * 30 + 1 AS INT)
               ),
               cast(rand() * 10 AS INT)
       )                                                          AS update_time,
       -- 生成原始数据JSON串
       concat(
               '{"activity_id":', id,
               ',"activity_name":"', CASE
                                         WHEN id % 8 = 0 THEN '新品特惠'
                                         WHEN id % 8 = 1 THEN '老客专享'
                                         WHEN id % 8 = 2 THEN '限时折扣'
                                         WHEN id % 8 = 3 THEN '满减活动'
                                         WHEN id % 8 = 4 THEN '会员回馈'
                                         WHEN id % 8 = 5 THEN '清仓处理'
                                         WHEN id % 8 = 6 THEN '周末特卖'
                                         ELSE '节日优惠'
                   END,
               '","activity_level":"', CASE WHEN id % 2 = 0 THEN '商品级' ELSE 'SKU级' END,
               '","activity_type":"', CASE WHEN id % 3 = 0 THEN '固定优惠' ELSE '自定义优惠' END,
               '"}'
       )                                                          AS raw_data
-- 直接通过子查询生成1-1000的自增ID
FROM (SELECT pos + 1 AS id
      FROM (SELECT posexplode(split(space(999), '')) AS (pos, val)) t) temp_activity_data;
    """

    print(f"[INFO] 开始执行SQL查询")
    df1 = spark.sql(select_sql1)  # 此时create_time和update_time为Timestamp类型

    # 不添加dt字段，保持6列与目标表一致
    df_without_partition = df1

    # 验证字段类型（关键：确认create_time和update_time为timestamp）
    print("[INFO] DataFrame字段类型：")
    df_without_partition.printSchema()
    print(f"[INFO] DataFrame列数: {len(df_without_partition.columns)}")  # 应输出6

    print(f"[INFO] SQL执行完成")
    df_without_partition.show(5)

    # 写入数据
    select_to_hive(df_without_partition, tableName)

    # 验证数据
    print(f"[INFO] 验证数据...")
    verify_df = spark.sql(f"SELECT * FROM {tableName} LIMIT 5")
    verify_df.show()


if __name__ == "__main__":
    target_date = '2025-08-03'
    execute_hive_insert(target_date, 'ods_activity_info')