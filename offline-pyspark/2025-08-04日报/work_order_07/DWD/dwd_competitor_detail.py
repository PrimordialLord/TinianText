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
    -- 本品ID（与商品基础表关联，确保非空）
    oci.product_id,

    -- 竞品ID（确保格式规范，非空）
    oci.competitor_product_id,

    -- 对比维度标准化（统一维度名称，便于分析）
    case oci.dimension
        when '价格' then '价格竞争力'
        when '销量' then '市场销量'
        when '好评率' then '用户好评率'
        when '功能完整性' then '产品功能'
        when '售后服务' then '服务质量'
        when '市场占有率' then '市场份额'
        when '用户复购率' then '用户忠诚度'
        when '品牌知名度' then '品牌影响力'
        else '其他维度' -- 统一非标准维度名称
        end                                                AS dimension,

    -- 本品指标值（清洗异常值，保留合理精度）
    round(
            case
                when oci.self_value < 0 then 0 -- 指标值不能为负
            -- 针对不同维度设置合理上限（根据业务常识）
                when oci.dimension in ('好评率', '用户复购率', '市场占有率') and oci.self_value > 100
                    then 100 -- 百分比类指标上限100%
                when oci.dimension in ('功能完整性', '售后服务') and oci.self_value > 10
                    then 10 -- 评分类指标上限10分
                else oci.self_value
                end, 2
    )                                                      AS self_value,

    -- 竞品指标值（同本品规则清洗）
    round(
            case
                when oci.competitor_value < 0 then 0
                when oci.dimension in ('好评率', '用户复购率', '市场占有率') and oci.competitor_value > 100
                    then 100
                when oci.dimension in ('功能完整性', '售后服务') and oci.competitor_value > 10
                    then 10
                else oci.competitor_value
                end, 2
    )                                                      AS competitor_value,

    -- DWD层ETL处理时间（记录转换时间）
    from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss') AS etl_time
FROM
    -- 源表：ODS层竞品对比原始表（指定分区）
    ods_competitor_info oci
WHERE
  -- 过滤当前分区数据
    oci.dt = '2025-08-03'
  -- 核心过滤：排除本品ID或竞品ID为空的无效数据
  AND oci.product_id IS NOT NULL
  AND oci.product_id != ''
  AND oci.competitor_product_id IS NOT NULL
  AND oci.competitor_product_id != ''
  -- 过滤无效维度（排除空维度或无意义值）
  AND oci.dimension IS NOT NULL
  AND oci.dimension NOT IN ('', '未知', '无');
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
    execute_hive_insert(target_date, 'dwd_competitor_detail')
