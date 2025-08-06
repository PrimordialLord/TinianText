USE work_order_03_double;

-- 商品360核心概览表（按日分区）
CREATE TABLE ads_product_360_overview (
                                          product_id STRING COMMENT '商品ID',
                                          commodity_name STRING COMMENT '商品名称',
                                          core_metrics STRING COMMENT '核心指标（JSON格式：访客数、支付金额等）',
                                          trend_analysis STRING COMMENT '趋势分析（JSON格式：近7天点击/销量趋势）',
                                          top_channel STRING COMMENT '主要流量渠道',
                                          hot_sku STRING COMMENT '热销SKU属性'
)
    PARTITIONED BY (stat_date STRING COMMENT '统计日期')
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS ORC;

-- 从DWS层各汇总表聚合生成ADS层商品360全景表（避免数组类型直接参与字符串拼接）
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE ads_product_360_overview PARTITION (stat_date = '2025-01-31')
SELECT
    core.product_id,
    '未知商品' AS commodity_name,
    -- 核心指标JSON（仅使用原始类型字段）
    CONCAT(
            '{"daily_click":', CAST(core.daily_click AS STRING),
            ',"daily_sales":', CAST(core.daily_sales AS STRING),
            ',"daily_sales_amount":', CAST(core.daily_sales_amount AS STRING),
            ',"visitor_ratio":', CAST(traffic.visitor_ratio AS STRING),
            ',"conversion_rate":', CAST(traffic.conversion_rate AS STRING),
            '}'
    ) AS core_metrics,
    -- 趋势分析：仅保留非数组类型的价格趋势
    CONCAT('{"price_trend":"', price.price_trend, '"}') AS trend_analysis,
    COALESCE(
            MAX(CASE WHEN traffic.channel_ranking = 1 THEN traffic.channel END),
            '无主要渠道'
    ) AS top_channel,
    COALESCE(
            COLLECT_LIST(CASE WHEN sku.is_hot = 1 THEN sku.attribute END)[0],
            '无热销SKU'
    ) AS hot_sku
FROM dws_product_core_summary core
         LEFT JOIN dws_sku_sales_summary sku
                   ON core.product_id = sku.product_id AND core.stat_date = sku.stat_date
         LEFT JOIN dws_price_analysis_summary price
                   ON core.product_id = price.product_id AND core.stat_date = price.stat_date
         LEFT JOIN dws_traffic_channel_summary traffic
                   ON core.product_id = traffic.product_id AND core.stat_date = traffic.stat_date
WHERE core.stat_date = '2025-01-31'
GROUP BY core.product_id,
         core.daily_click, core.daily_sales, core.daily_sales_amount,
         price.price_trend, traffic.visitor_ratio, traffic.conversion_rate;

select * from ads_product_360_overview;
-- 价格力分析表（按日分区）
CREATE TABLE ads_price_strength_analysis (
                                             product_id STRING COMMENT '商品ID',
                                             price_strength_star INT COMMENT '价格力星级（1-5星）',
                                             price_band STRING COMMENT '价格带',
                                             same_star_benchmark STRING COMMENT '同星级商品标杆数据（JSON）',
                                             price_strategy_suggestion STRING COMMENT '定价策略建议'
)
    PARTITIONED BY (stat_date STRING COMMENT '统计日期')
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS ORC;

-- 从DWS层价格分析表聚合生成ADS层价格力分析表（修正GROUP BY字段缺失问题）
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE ads_price_strength_analysis PARTITION (stat_date = '2025-01-31')
SELECT
    price.product_id,
    price.star AS price_strength_star,
    price.price_band,
    -- 同星级商品标杆数据JSON（使用聚合函数而非窗口函数）
    CONCAT(
            '{"avg_sales":', CAST(AVG(core.daily_sales) AS STRING),
            ',"avg_conversion":', CAST(AVG(core.daily_sales / NULLIF(core.daily_click, 0)) AS STRING),
            '}'
    ) AS same_star_benchmark,
    -- 定价策略建议
    CASE
        WHEN price.price_trend = '升' AND price.current_price > price.same_band_avg_price THEN '建议维持价格，优化增值服务'
        WHEN price.price_trend = '降' AND price.current_price < price.same_band_avg_price THEN '建议小幅回升，提升利润空间'
        ELSE '建议参考同价格带平均价格，保持竞争力'
        END AS price_strategy_suggestion
FROM (
         -- 子查询：计算价格力星级
         SELECT
             *,
             CASE
                 WHEN CAST(current_price AS BIGINT) <= PERCENTILE(CAST(current_price AS BIGINT), 0.2) OVER (PARTITION BY category_id, price_band) THEN 5
                 WHEN CAST(current_price AS BIGINT) <= PERCENTILE(CAST(current_price AS BIGINT), 0.4) OVER (PARTITION BY category_id, price_band) THEN 4
                 WHEN CAST(current_price AS BIGINT) <= PERCENTILE(CAST(current_price AS BIGINT), 0.6) OVER (PARTITION BY category_id, price_band) THEN 3
                 WHEN CAST(current_price AS BIGINT) <= PERCENTILE(CAST(current_price AS BIGINT), 0.8) OVER (PARTITION BY category_id, price_band) THEN 2
                 ELSE 1
                 END AS star
         FROM dws_price_analysis_summary
         WHERE stat_date = '2025-01-31'
     ) price
         LEFT JOIN dws_product_core_summary core
                   ON price.product_id = core.product_id AND price.stat_date = core.stat_date
GROUP BY price.product_id, price.price_band, price.current_price, price.price_trend,
         price.same_band_avg_price, price.category_id, price.star;

select * from ads_price_strength_analysis;

-- 评价体验分析表（按日分区）
CREATE TABLE ads_evaluation_experience (
                                           product_id STRING COMMENT '商品ID',
                                           score_distribution STRING COMMENT '评分分布（JSON格式）',
                                           positive_rate DOUBLE COMMENT '正面评价率',
                                           key_complaint STRING COMMENT '主要投诉点（如质量、物流）',
                                           evaluation_trend STRING COMMENT '评价趋势（近30天）'
)
    PARTITIONED BY (stat_date STRING COMMENT '统计日期')
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS ORC;


