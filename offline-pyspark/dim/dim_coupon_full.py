from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

def get_spark_session():
    """初始化SparkSession并配置Hive连接，确保数据库存在"""
    # 创建SparkSession并配置Hive元数据服务地址
    spark = SparkSession.builder \
        .appName("HiveETL") \
        .config("spark.hadoop.hive.metastore.uris", "thrift://cdh01:9083") \
        .config("spark.sql.hive.convertMetastoreOrc", "true") \
        .config("hive.exec.dynamic.partition.mode", "nonstrict") \
        .config("spark.sql.hive.ignoreMissingPartitions", "true") \
        .enableHiveSupport() \
        .getOrCreate()

    # 设置日志级别为WARN，减少不必要的输出
    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    # 创建目标数据库（如果不存在）并切换到该数据库
    spark.sql("CREATE DATABASE IF NOT EXISTS gmall")
    spark.sql("USE gmall")

    return spark


def select_to_hive(jdbcDF, tableName):
    """
    将DataFrame数据追加写入Hive表
    参数:
        jdbcDF: 待写入的DataFrame
        tableName: 目标Hive表名（不含数据库前缀）
    """
    # 写入模式为append，自动处理分区
    jdbcDF.write.mode('append').insertInto(f"{tableName}")


def execute_hive_insert(partition_date: str, tableName):
    """
    从源表查询数据并写入目标Hive分区表
    参数:
        partition_date: 分区日期（格式如'20211214'）
        tableName: 目标Hive表名
    """
    # 获取已配置的SparkSession
    spark = get_spark_session()

    # 构建SQL查询语句，从ods_promotion_refer表获取指定日期的数据
    # 注意：此处硬编码了源表分区日期，需根据实际情况调整
    select_sql1 = f"""
    select
    id,
    coupon_name,
    coupon_type,
    coupon_dic.dic_name,
    condition_amount,
    condition_num,
    activity_id,
    benefit_amount,
    benefit_discount,
    case coupon_type
        when '3201' then concat('满',condition_amount,'元减',benefit_amount,'元')
        when '3202' then concat('满',condition_num,'件打', benefit_discount,' 折')
        when '3203' then concat('减',benefit_amount,'元')
        end benefit_rule,
    create_time,
    range_type,
    range_dic.dic_name,
    limit_num,
    taken_count,
    start_time,
    end_time,
    operate_time,
    expire_time
from
    (
        select
            id,
            coupon_name,
            coupon_type,
            condition_amount,
            condition_num,
            activity_id,
            benefit_amount,
            benefit_discount,
            create_time,
            range_type,
            limit_num,
            taken_count,
            start_time,
            end_time,
            operate_time,
            expire_time
        from ods_coupon_info
        where ds='20220602'
    )ci
        left join
    (
        select
            dic_code,
            dic_name
        from ods_base_dic
        where ds='20211214'
          and parent_code='32'
    )coupon_dic
    on ci.coupon_type=coupon_dic.dic_code
        left join
    (
        select
            dic_code,
            dic_name
        from ods_base_dic
        where ds='20211214'
          and parent_code='33'
    )range_dic
    on ci.range_type=range_dic.dic_code; -- 此处硬编码了源表分区，需改为动态参数
    """

    print(f"[INFO] 开始执行SQL查询，目标分区：{partition_date}")
    df1 = spark.sql(select_sql1)

    # 添加分区字段dt，并设置为传入的partition_date
    # 注意：确保目标表使用dt作为分区字段名
    df_with_partition = df1.withColumn("ds", lit(partition_date))

    print(f"[INFO] SQL执行完成，分区{partition_date}操作成功")
    df_with_partition.show(5)  # 显示前5条数据用于调试

    # 将处理后的数据写入Hive表
    select_to_hive(df_with_partition, tableName)


if __name__ == "__main__":
    # 设置目标分区日期
    target_date = '20211214'
    # 执行数据ETL，从ods表到dim表
    execute_hive_insert(target_date, 'dim_coupon_full')