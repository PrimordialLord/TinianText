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
SELECT concat('prod_', lower(substr(md5(cast(rand() as string)), 1, 8))) AS product_id,
       concat(
           -- 简化随机前缀选择
               case floor(rand() * 7)
                   when 0 then '新款'
                   when 1 then '精选'
                   when 2 then '优质'
                   when 3 then '高级'
                   when 4 then '特惠'
                   when 5 then '限量版'
                   else '经典'
                   end, ' ',
               c.category_name, ' ',
               case c.category_name
                   when '电子产品' then
                       case floor(rand() * 5)
                           when 0 then '手机'
                           when 1 then '电脑'
                           when 2 then '平板'
                           when 3 then '耳机'
                           else '手表'
                           end
                   when '服装鞋帽' then
                       case floor(rand() * 5)
                           when 0 then 'T恤'
                           when 1 then '牛仔裤'
                           when 2 then '运动鞋'
                           when 3 then '衬衫'
                           else '外套'
                           end
                   when '家居用品' then
                       case floor(rand() * 5)
                           when 0 then '沙发'
                           when 1 then '桌子'
                           when 2 then '椅子'
                           when 3 then '台灯'
                           else '被子'
                           end
                   when '食品饮料' then
                       case floor(rand() * 5)
                           when 0 then '饼干'
                           when 1 then '巧克力'
                           when 2 then '饮料'
                           when 3 then '水果'
                           else '面包'
                           end
                   else
                       case floor(rand() * 5)
                           when 0 then '口红'
                           when 1 then '面膜'
                           when 2 then '洗发水'
                           when 3 then '沐浴露'
                           else '面霜'
                           end
                   end, ' ',
               case floor(rand() * 5)
                   when 0 then '升级版'
                   when 1 then '超值装'
                   when 2 then '豪华款'
                   when 3 then '基础款'
                   else '专业版'
                   end
       )                                                                 AS product_name,
       c.category_id,
       c.category_name,
       b.brand_id,
       b.brand_name,
       round(9.9 + rand() * (9999.9 - 9.9), 2)                           AS price,
       concat(
           '{"product_id":"', concat('prod_', lower(substr(md5(cast(rand() as string)), 1, 8))), '",',
           '"product_name":"', concat(
                   case floor(rand() * 7)
                       when 0 then '新款'
                       when 1 then '精选'
                       else '优质'
                       end, ' ',
                   c.category_name, ' ',
                   case c.category_name
                       when '电子产品' then
                           case floor(rand() * 5)
                               when 0 then '手机'
                               else '电脑'
                               end
                       else '通用商品'
                       end
                               ), '",',
           -- 关键修改：将BIGINT转换为INT类型
           '"create_time":"', date_sub(current_date(), cast(floor(rand() * 365) as int)), ' ',
           lpad(floor(rand() * 24), 2, '0'), ':',
           lpad(floor(rand() * 60), 2, '0'), ':',
           lpad(floor(rand() * 60), 2, '0'), '",',
           '"status":"',
           case floor(rand() * 3)
               when 0 then '在售'
               when 1 then '下架'
               else '预售'
               end, '"',
           '}'
       )                                                                 AS raw_data,
       from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss')            AS etl_time
FROM (SELECT 1 AS dummy) t
         CROSS JOIN (SELECT posexplode(split(space(99), '')) AS (id, val)) nums -- 生成100条数据
         CROSS JOIN (SELECT category_id, category_name
                     FROM (SELECT stack(5,
                                        'cat_101', '电子产品',
                                        'cat_201', '服装鞋帽',
                                        'cat_301', '家居用品',
                                        'cat_401', '食品饮料',
                                        'cat_501', '美妆个护'
                                  ) AS (category_id, category_name)) categories
                     ORDER BY rand()
                     LIMIT 1) c
         CROSS JOIN (SELECT brand_id, brand_name
                     FROM (SELECT stack(7,
                                        'brand_11', '品牌A',
                                        'brand_12', '品牌B',
                                        'brand_13', '品牌C',
                                        'brand_14', '品牌D',
                                        'brand_15', '品牌E',
                                        'brand_16', '品牌F',
                                        'brand_17', '品牌G'
                                  ) AS (brand_id, brand_name)) brands
                     ORDER BY rand()
                     LIMIT 1) b;
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
    execute_hive_insert(target_date, 'ods_product_base')
