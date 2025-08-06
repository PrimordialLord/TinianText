USE work_order_03_double;

show tables;
-- 1. 商品核心明细（按日分区，支持核心概况分析）
CREATE TABLE dwd_product_core_detail (
                                         product_id STRING COMMENT '商品ID',
                                         click_num INT COMMENT '点击量',
                                         sales_num INT COMMENT '销量',
                                         sales_amount DOUBLE COMMENT '销售额',
                                         op_action STRING COMMENT '运营行动（如报名新客折扣、发布短视频）' -- 对应文档中运营节点支持需求
)
    PARTITIONED BY (stat_date STRING COMMENT '统计日期') -- 分区字段单独定义，不与表字段重复
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS ORC;

-- 从ODS层商品核心日志表抽取数据到DWD层商品核心详情表，按统计日期分区（关闭动态分区严格模式）
-- 临时关闭动态分区严格模式（适用于非生产环境快速测试，生产环境建议明确指定静态分区）
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE dwd_product_core_detail PARTITION (stat_date)
SELECT
    product_id,
    SUM(click_num) AS click_num,  -- 汇总每日点击量
    SUM(sales_num) AS sales_num,  -- 汇总每日销量
    SUM(sales_amount) AS sales_amount,  -- 汇总每日销售额
    MAX(op_action) AS op_action,  -- 取当日最后一次运营行动
    SUBSTR(create_time, 1, 10) AS stat_date  -- 提取日期作为分区字段
FROM ods_product_core_log
GROUP BY product_id, SUBSTR(create_time, 1, 10);

select * from dwd_product_core_detail;

-- 2. SKU销售明细（按日分区，支持SKU销售详情分析）
CREATE TABLE dwd_sku_sales_detail (
                                      sku_id STRING COMMENT 'SKU ID',
                                      product_id STRING COMMENT '商品ID',
                                      attribute STRING COMMENT '商品属性（如颜色、尺寸）',
                                      sales_num INT COMMENT '销量'
)
    PARTITIONED BY (stat_date STRING COMMENT '统计日期')
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS ORC;

-- 从ODS层SKU销售日志表抽取数据到DWD层SKU销售详情表，按统计日期分区
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE dwd_sku_sales_detail PARTITION (stat_date)
SELECT
    sku_id,
    product_id,
    attribute,
    SUM(sales_num) AS sales_num,  -- 汇总每日SKU销量
    SUBSTR(create_time, 1, 10) AS stat_date  -- 提取日期作为分区字段
FROM ods_sku_sales_log
GROUP BY sku_id, product_id, attribute, SUBSTR(create_time, 1, 10);

select * from dwd_sku_sales_detail;

-- 3. 价格明细（按日分区，支持价格分析）
CREATE TABLE dwd_price_detail (
                                  product_id STRING COMMENT '商品ID',
                                  price DOUBLE COMMENT '商品价格',
                                  category_id STRING COMMENT '类目ID',
                                  price_change_flag TINYINT COMMENT '价格变动标识（1-变动，0-不变）'
)
    PARTITIONED BY (stat_date STRING COMMENT '统计日期')
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS ORC;

-- 从ODS层价格日志表抽取数据到DWD层价格详情表，按统计日期分区
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE dwd_price_detail PARTITION (stat_date)
SELECT
    product_id,
    price,
    category_id,
    -- 标记价格是否变动（对比当日与前一日价格）
    CASE
        WHEN LAG(price) OVER (PARTITION BY product_id ORDER BY price_time) != price THEN 1
        ELSE 0
        END AS price_change_flag,
    SUBSTR(price_time, 1, 10) AS stat_date  -- 提取日期作为分区字段
FROM ods_price_log;

select * from dwd_price_detail;

-- 4. 流量来源明细（按日分区，支持流量渠道分析）
CREATE TABLE dwd_traffic_source_detail (
                                           product_id STRING COMMENT '商品ID',
                                           channel STRING COMMENT '流量渠道',
                                           visitor_num INT COMMENT '访客数',
                                           conversion_num INT COMMENT '转化数'
)
    PARTITIONED BY (stat_date STRING COMMENT '统计日期')
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS ORC;

-- 从ODS层流量来源日志表抽取数据到DWD层流量来源详情表，按统计日期分区
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE dwd_traffic_source_detail PARTITION (stat_date)
SELECT
    product_id,
    channel,
    SUM(visitor_num) AS visitor_num,  -- 汇总每日各渠道访客数
    SUM(conversion_num) AS conversion_num,  -- 汇总每日各渠道转化数
    SUBSTR(log_time, 1, 10) AS stat_date  -- 提取日期作为分区字段
FROM ods_traffic_source_log
GROUP BY product_id, channel, SUBSTR(log_time, 1, 10);

select * from dwd_traffic_source_detail;
