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
    -- 本品ID（与商品表关联，格式统一）
    concat('prod_', lower(substr(md5(cast(rand() as string)), 1, 8)))           AS product_id,

    -- 竞品ID（独立格式，区分本品）
    concat('comp_', lower(substr(md5(cast(rand() * 1000000 as string)), 1, 8))) AS competitor_product_id,

    -- 对比维度（随机选择常见竞品分析维度）
    case floor(rand() * 8)
        when 0 then '价格'
        when 1 then '销量'
        when 2 then '好评率'
        when 3 then '功能完整性'
        when 4 then '售后服务'
        when 5 then '市场占有率'
        when 6 then '用户复购率'
        else '品牌知名度'
        end                                                                     AS dimension,

    -- 本品指标值（根据维度生成合理范围）
    case floor(rand() * 8)
        when 0 then round(50 + rand() * 9950, 2) -- 价格：50-10000
        when 1 then round(10 + rand() * 9990, 0) -- 销量：10-10000
        when 2 then round(80 + rand() * 20, 2) -- 好评率：80%-100%
        when 3 then round(3 + rand() * 7, 1) -- 功能完整性：3-10分
        when 4 then round(3 + rand() * 7, 1) -- 售后服务：3-10分
        when 5 then round(5 + rand() * 30, 2) -- 市场占有率：5%-35%
        when 6 then round(10 + rand() * 50, 2) -- 用户复购率：10%-60%
        else round(40 + rand() * 60, 2) -- 品牌知名度：40-100分
        end                                                                     AS self_value,

    -- 竞品指标值（与本品有差异，模拟竞争关系）
    case floor(rand() * 8)
        when 0 then round(
                (50 + rand() * 9950) * (0.8 + rand() * 0.4) -- 价格：本品的80%-120%
            , 2)
        when 1 then round(
                (10 + rand() * 9990) * (0.5 + rand() * 1) -- 销量：本品的50%-150%
            , 0)
        when 2 then round(
                (80 + rand() * 20) * (0.8 + rand() * 0.4) -- 好评率：本品的80%-120%
            , 2)
        else round(
                (3 + rand() * 7) * (0.7 + rand() * 0.6) -- 其他指标：本品的70%-130%
            , 1)
        end                                                                     AS competitor_value,

    -- 原始JSON数据（包含完整对比信息）
    concat(
            '{"product_id":"', concat('prod_', lower(substr(md5(cast(rand() as string)), 1, 8))), '",',
            '"competitor_product_id":"', concat('comp_', lower(substr(md5(cast(rand() * 1000000 as string)), 1, 8))),
            '",',
            '"dimension":"', case floor(rand() * 8)
                                 when 0 then '价格'
                                 when 1 then '销量'
                                 else '好评率'
                end, '",',
            '"self_value":', case floor(rand() * 8)
                                 when 0 then round(50 + rand() * 9950, 2)
                                 else round(80 + rand() * 20, 2)
                end, ',',
            '"competitor_value":', case floor(rand() * 8)
                                       when 0 then round((50 + rand() * 9950) * (0.8 + rand() * 0.4), 2)
                                       else round((80 + rand() * 20) * (0.8 + rand() * 0.4), 2)
                end, ',',
            '"compare_result":"', case
                                      when (case floor(rand() * 8)
                                                when 0 then 50 + rand() * 9950
                                                else 80 + rand() * 20 end)
                                          > (case floor(rand() * 8)
                                                 when 0 then (50 + rand() * 9950) * (0.8 + rand() * 0.4)
                                                 else (80 + rand() * 20) * (0.8 + rand() * 0.4) end)
                                          then '本品占优'
                                      else '竞品占优'
                end, '",',
            '"collect_time":"', current_date(), ' ',
            lpad(floor(rand() * 24), 2, '0'), ':',
            lpad(floor(rand() * 60), 2, '0'), ':',
            lpad(floor(rand() * 60), 2, '0'), '"'
    )                                                                           AS raw_data,

    -- ETL处理时间（当前时间）
    from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss')                      AS etl_time
FROM
    -- 生成200条数据（可调整space参数控制数量）
    (SELECT posexplode(split(space(199), '')) AS (id, val)) nums;
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
    execute_hive_insert(target_date, 'ods_competitor_info')
