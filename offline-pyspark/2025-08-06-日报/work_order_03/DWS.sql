USE work_order_03_double;

-- 商品核心指标汇总（按日分区）
CREATE TABLE dws_product_core_summary (
                                          product_id STRING COMMENT '商品ID',
                                          daily_click INT COMMENT '当日点击量',
                                          daily_sales INT COMMENT '当日销量',
                                          daily_sales_amount DOUBLE COMMENT '当日销售额',
                                          7d_click_trend ARRAY<INT> COMMENT '近7天点击趋势',
                                          7d_sales_trend ARRAY<INT> COMMENT '近7天销量趋势'
)
    PARTITIONED BY (stat_date STRING COMMENT '统计日期')
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS ORC;

-- 重新生成DWS层商品核心汇总表数据，解决空数据问题
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE dws_product_core_summary PARTITION (stat_date = '2025-01-31')
SELECT
    all_products.product_id,
    -- 当日核心指标（为空时填充0）
    COALESCE(MAX(CASE WHEN temp_data.stat_date = '2025-01-31' THEN temp_data.click_num END), 0) AS daily_click,
    COALESCE(MAX(CASE WHEN temp_data.stat_date = '2025-01-31' THEN temp_data.sales_num END), 0) AS daily_sales,
    COALESCE(MAX(CASE WHEN temp_data.stat_date = '2025-01-31' THEN temp_data.sales_amount END), 0.0) AS daily_sales_amount,
    -- 趋势数组为空时填充默认空整数数组（匹配COLLECT_LIST返回的int数组类型）
    COALESCE(temp_data.click_trend, ARRAY(CAST(NULL AS INT))) AS 7d_click_trend,
    COALESCE(temp_data.sales_trend, ARRAY(CAST(NULL AS INT))) AS 7d_sales_trend
FROM (
         -- 主查询：获取所有商品ID
         SELECT DISTINCT product_id FROM dwd_product_core_detail
     ) all_products
-- 左连接包含详细数据的子查询
         LEFT JOIN (
    SELECT
        dwd.product_id,
        dwd.stat_date,
        dwd.click_num,
        dwd.sales_num,
        dwd.sales_amount,
        temp.click_trend,
        temp.sales_trend
    FROM dwd_product_core_detail dwd
             JOIN (
        SELECT
            product_id,
            stat_date,
            COLLECT_LIST(click_num) OVER (
                PARTITION BY product_id
                ORDER BY stat_date
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
                ) AS click_trend,
            COLLECT_LIST(sales_num) OVER (
                PARTITION BY product_id
                ORDER BY stat_date
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
                ) AS sales_trend
        FROM dwd_product_core_detail
        WHERE stat_date >= DATE_SUB('2025-01-31', 1000)
          AND stat_date <= '2025-01-31'
    ) temp ON dwd.product_id = temp.product_id AND dwd.stat_date = temp.stat_date
    WHERE temp.stat_date = '2025-01-31'
) temp_data ON all_products.product_id = temp_data.product_id
GROUP BY all_products.product_id, temp_data.click_trend, temp_data.sales_trend;

select * from dws_product_core_summary;
-- SKU销售汇总（按日分区）
CREATE TABLE dws_sku_sales_summary (
                                       product_id STRING COMMENT '商品ID',
                                       attribute STRING COMMENT '商品属性',
                                       total_sales INT COMMENT '累计销量',
                                       sales_ratio DOUBLE COMMENT '销量占比（属性维度）',
                                       is_hot TINYINT COMMENT '是否热销（1-是，0-否）'
)
    PARTITIONED BY (stat_date STRING COMMENT '统计日期')
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS ORC;

-- 从DWD层SKU销售详情表聚合生成DWS层SKU销售汇总表，支持SKU热销分析
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE dws_sku_sales_summary PARTITION (stat_date = '2025-01-31')
SELECT
    all_skus.product_id,
    all_skus.attribute,
    -- 累计销量为空时填充0
    COALESCE(SUM(temp_data.sales_num), 0) AS total_sales,
    -- 销量占比为空时填充0.0
    COALESCE(
            SUM(temp_data.sales_num) / SUM(SUM(temp_data.sales_num)) OVER (PARTITION BY all_skus.product_id),
            0.0
    ) AS sales_ratio,
    -- 非热销或无数据时标记为0
    COALESCE(
            CASE
                WHEN PERCENT_RANK() OVER (
                    PARTITION BY all_skus.product_id
                    ORDER BY SUM(temp_data.sales_num) DESC
                    ) <= 0.3 THEN 1
                ELSE 0
                END,
            0
    ) AS is_hot
FROM (
         -- 主查询：获取所有商品的SKU属性组合，确保全覆盖
         SELECT DISTINCT product_id, attribute
         FROM dwd_sku_sales_detail
     ) all_skus
-- 左连接实际销售数据，避免因无销量数据丢失SKU属性组合
         LEFT JOIN (
    SELECT
        product_id,
        attribute,
        sales_num
    FROM dwd_sku_sales_detail
    WHERE stat_date = '2025-01-31'
) temp_data ON all_skus.product_id = temp_data.product_id
    AND all_skus.attribute = temp_data.attribute
GROUP BY all_skus.product_id, all_skus.attribute;

select * from dws_sku_sales_summary;
-- 价格分析汇总（按日分区）
CREATE TABLE dws_price_analysis_summary (
                                            product_id STRING COMMENT '商品ID',
                                            category_id STRING COMMENT '类目ID',
                                            current_price DOUBLE COMMENT '当前价格',
                                            price_band STRING COMMENT '价格带（如0~50、51~100）',
                                            price_trend STRING COMMENT '价格趋势（升/降/平）',
                                            same_band_avg_price DOUBLE COMMENT '同价格带平均价格'
)
    PARTITIONED BY (stat_date STRING COMMENT '统计日期')
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS ORC;

-- 从DWD层价格详情表聚合生成DWS层价格分析汇总表，支持价格带及趋势分析
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE dws_price_analysis_summary PARTITION (stat_date = '2025-01-31')
SELECT
    all_products.product_id,
    COALESCE(temp_data.category_id, '') AS category_id,
    COALESCE(temp_data.current_price, 0.0) AS current_price,
    COALESCE(temp_data.price_band, '未知') AS price_band,
    COALESCE(temp_data.price_trend, '平') AS price_trend,
    COALESCE(temp_data.same_band_avg_price, 0.0) AS same_band_avg_price
FROM (
         -- 主查询：获取所有商品ID，确保全覆盖
         SELECT DISTINCT product_id FROM dwd_price_detail
     ) all_products
-- 左连接价格分析数据，避免因无数据丢失商品
         LEFT JOIN (
    SELECT
        product_id,
        category_id,
        price AS current_price,
        -- 划分价格带
        CASE
            WHEN price <= 50 THEN '0~50'
            WHEN price <= 100 THEN '51~100'
            WHEN price <= 500 THEN '101~500'
            ELSE '501+'
            END AS price_band,
        -- 判定价格趋势（对比当前与7天前价格）
        CASE
            WHEN price > LAG(price, 7) OVER (PARTITION BY product_id ORDER BY stat_date) THEN '升'
            WHEN price < LAG(price, 7) OVER (PARTITION BY product_id ORDER BY stat_date) THEN '降'
            ELSE '平'
            END AS price_trend,
        -- 同价格带平均价格
        AVG(price) OVER (PARTITION BY category_id,
            CASE
                WHEN price <= 50 THEN '0~50'
                WHEN price <= 100 THEN '51~100'
                WHEN price <= 500 THEN '101~500'
                ELSE '501+'
                END
            ) AS same_band_avg_price
    FROM dwd_price_detail
    WHERE stat_date = '2025-01-31'
) temp_data ON all_products.product_id = temp_data.product_id
GROUP BY all_products.product_id, temp_data.category_id, temp_data.current_price,
         temp_data.price_band, temp_data.price_trend, temp_data.same_band_avg_price;

select * from dws_price_analysis_summary;

-- 流量渠道效果汇总（按日分区）
CREATE TABLE dws_traffic_channel_summary (
                                             product_id STRING COMMENT '商品ID',
                                             channel STRING COMMENT '流量渠道',
                                             visitor_ratio DOUBLE COMMENT '访客占比',
                                             conversion_rate DOUBLE COMMENT '转化率',
                                             channel_ranking INT COMMENT '渠道效果排名'
)
    PARTITIONED BY (stat_date STRING COMMENT '统计日期')
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS ORC;

-- 从DWD层流量来源详情表聚合生成DWS层流量渠道汇总表，支持流量渠道分析
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE dws_traffic_channel_summary PARTITION (stat_date = '2025-01-31')
SELECT
    all_channels.product_id,
    all_channels.channel,
    -- 访客占比：为空时填充0.0
    COALESCE(
            SUM(temp_data.visitor_num) / SUM(SUM(temp_data.visitor_num)) OVER (PARTITION BY all_channels.product_id),
            0.0
    ) AS visitor_ratio,
    -- 转化率：为空时填充0.0
    COALESCE(
            SUM(temp_data.conversion_num) / NULLIF(SUM(temp_data.visitor_num), 0),
            0.0
    ) AS conversion_rate,
    -- 渠道效果排名：无数据时填充0
    COALESCE(
            RANK() OVER (
                PARTITION BY all_channels.product_id
                ORDER BY SUM(temp_data.conversion_num) / NULLIF(SUM(temp_data.visitor_num), 0) DESC
                ),
            0
    ) AS channel_ranking
FROM (
         -- 主查询：获取所有商品的流量渠道组合，确保全覆盖
         SELECT DISTINCT product_id, channel
         FROM dwd_traffic_source_detail
     ) all_channels
-- 左连接实际流量数据，避免因无数据丢失渠道
         LEFT JOIN (
    SELECT
        product_id,
        channel,
        visitor_num,
        conversion_num
    FROM dwd_traffic_source_detail
    WHERE stat_date = '2025-01-31'
) temp_data ON all_channels.product_id = temp_data.product_id
    AND all_channels.channel = temp_data.channel
GROUP BY all_channels.product_id, all_channels.channel;

select * from dws_traffic_channel_summary;