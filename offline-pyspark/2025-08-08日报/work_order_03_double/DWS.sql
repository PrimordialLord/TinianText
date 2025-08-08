USE rewrite_work_order_03;

-- TODO 1. 商品每日汇总表（dws_product_daily_summary）
DROP TABLE IF EXISTS dws_product_daily_summary;
CREATE EXTERNAL TABLE IF NOT EXISTS dws_product_daily_summary
(
    product_id       STRING COMMENT '商品ID',
    category_id      STRING COMMENT '类目ID',
    -- 销售指标
    pay_amount       DECIMAL(16, 2) COMMENT '当日支付金额',
    pay_num          BIGINT COMMENT '当日支付件数',
    avg_price        DECIMAL(10, 2) COMMENT '当日平均成交价',
    -- 流量指标
    pv               BIGINT COMMENT '当日浏览量',
    uv               BIGINT COMMENT '当日访客数',
    avg_stay_time    INT COMMENT '平均停留时长（秒）',
    -- 渠道占比
    channel_uv_ratio MAP<STRING,DOUBLE> COMMENT '各渠道访客占比',
    -- 评价指标
    review_count     BIGINT COMMENT '当日新增评价数',
    avg_score        DOUBLE COMMENT '当日新增评价平均分'
)
    COMMENT '商品每日汇总指标表'
    PARTITIONED BY (dt STRING)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
        WITH SERDEPROPERTIES (
        'field.delim' = '\t',
        'serialization.format' = '\t'
        )
    STORED AS ORC
    LOCATION '/user/hive/warehouse/dws_db/dws_product_daily_summary';

-- 数据聚合SQL（关联用户行为和评价表）
INSERT OVERWRITE TABLE dws_product_daily_summary PARTITION (dt = '${bizdate}')
SELECT p.product_id,
       p.category_id,
       -- 销售指标（支付行为）
       SUM(CASE WHEN b.behavior_type = '支付' THEN s.sku_price ELSE 0 END)             AS pay_amount,
       SUM(CASE WHEN b.behavior_type = '支付' THEN 1 ELSE 0 END)                       AS pay_num,
       AVG(CASE WHEN b.behavior_type = '支付' THEN s.sku_price ELSE NULL END)          AS avg_price,
       -- 流量指标（浏览行为）
       SUM(CASE WHEN b.behavior_type = '浏览' THEN 1 ELSE 0 END)                       AS pv,
       COUNT(DISTINCT CASE WHEN b.behavior_type = '浏览' THEN b.user_id ELSE NULL END) AS uv,
       30                                                                              AS avg_stay_time, -- 示例：实际需根据行为日志计算
       -- 渠道占比（按UV分组计算）
       map_from_entries(
               collect_list(
                       named_struct('key', b.channel, 'value',
                                    COUNT(DISTINCT b.user_id) /
                                    COUNT(DISTINCT CASE WHEN b.behavior_type = '浏览' THEN b.user_id ELSE NULL END)
                       )
               )
       )                                                                               AS channel_uv_ratio,
       -- 评价指标
       COUNT(DISTINCT r.review_id)                                                     AS review_count,
       AVG(r.score)                                                                    AS avg_score
FROM dwd_product_detail p
         LEFT JOIN dwd_user_behavior_detail b
                   ON p.product_id = b.product_id AND b.dt = '${bizdate}'
         LEFT JOIN dwd_sku_detail s
                   ON b.sku_id = s.sku_id AND s.dt = '${bizdate}'
         LEFT JOIN dwd_review_detail r
                   ON p.product_id = r.product_id AND r.dt = '${bizdate}'
WHERE p.dt = '${bizdate}'
GROUP BY p.product_id, p.category_id;

-- TODO 2. SKU 每日汇总表（dws_sku_daily_summary）
DROP TABLE IF EXISTS dws_sku_daily_summary;
CREATE EXTERNAL TABLE IF NOT EXISTS dws_sku_daily_summary
(
    sku_id       STRING COMMENT 'SKU ID',
    product_id   STRING COMMENT '商品ID',
    sku_attr_map MAP<STRING,STRING> COMMENT 'SKU属性（如{"颜色":"红色","尺寸":"XL"}）',
    sales_num    BIGINT COMMENT '当日销量',
    sales_ratio  DOUBLE COMMENT '销量占商品总销量比例',
    stock_num    BIGINT COMMENT '当日库存',
    avg_score    DOUBLE COMMENT 'SKU当日平均评分'
)
    COMMENT 'SKU每日汇总指标表'
    PARTITIONED BY (dt STRING)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
        WITH SERDEPROPERTIES (
        'field.delim' = '\t',
        'serialization.format' = '\t'
        )
    STORED AS ORC; -- 采用ORC格式优化存储和查询效率