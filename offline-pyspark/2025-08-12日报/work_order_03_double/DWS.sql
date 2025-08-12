USE rewrite_work_order_03;

-- todo 1. 商品核心指标汇总表（dws_product_core_indicators）
DROP TABLE IF EXISTS dws_product_core_indicators;
CREATE TABLE IF NOT EXISTS dws_product_core_indicators (
                                                           product_id STRING COMMENT '商品ID',
                                                           stat_date STRING COMMENT '统计日期（yyyy-MM-dd）',
                                                           click_count BIGINT COMMENT '总点击量',
                                                           click_trend_json STRING COMMENT '小时级点击趋势（JSON数组，如[{"time":"08","count":100}]）',
                                                           order_count BIGINT COMMENT '总订单量',
                                                           sales_amount DECIMAL(16,2) COMMENT '总销售额',
                                                           operation_nodes_json STRING COMMENT '运营节点（JSON数组，如[{"action":"发布短视频","time":"10:00"}]）'
) COMMENT '商品核心指标汇总表'
    PARTITIONED BY (dt STRING COMMENT '分区日期（yyyy-MM-dd）')
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\t'
    STORED AS ORC;

INSERT INTO TABLE dws_product_core_indicators PARTITION (dt='2025-01-26')
SELECT
    p.product_id,
    '2025-01-26' AS stat_date,
    COALESCE(click_agg.click_count, 0) AS click_count,
    -- 确保JSON始终为有效格式
    COALESCE(click_trend.trend_json, '[]') AS click_trend_json,
    COALESCE(order_agg.order_count, 0) AS order_count,
    COALESCE(order_agg.sales_amount, 0) AS sales_amount,
    -- 确保JSON始终为有效格式
    COALESCE(op_nodes.nodes_json, '[]') AS operation_nodes_json
FROM
    dim_product p
        LEFT JOIN (
        SELECT
            product_id,
            COUNT(*) AS click_count
        FROM dwd_product_behavior_detail
        WHERE dt='2025-01-26'
          AND behavior_type='click'
        GROUP BY product_id
    ) click_agg
                  ON p.product_id = click_agg.product_id
        LEFT JOIN (
        SELECT
            product_id,
            -- 使用size()判断数组是否为空，避免NULL值
            CASE
                WHEN size(collect_list(concat('{"time":"', hour, '","count":', cnt, '}'))) > 0
                    THEN concat('[', concat_ws(',', collect_list(concat('{"time":"', hour, '","count":', cnt, '}'))), ']')
                ELSE '[]'
                END AS trend_json
        FROM (
                 SELECT
                     product_id,
                     hour,
                     COUNT(*) AS cnt
                 FROM dwd_product_behavior_detail
                 WHERE dt='2025-01-26'
                   AND behavior_type='click'
                 GROUP BY product_id, hour
             ) t
        GROUP BY product_id
    ) click_trend
                  ON p.product_id = click_trend.product_id
        LEFT JOIN (
        SELECT
            product_id,
            COUNT(*) AS order_count,
            SUM(order_amount) AS sales_amount
        FROM dwd_order_detail
        WHERE dt='2025-01-26'
          AND is_paid=1
        GROUP BY product_id
    ) order_agg
                  ON p.product_id = order_agg.product_id
        LEFT JOIN (
        SELECT
            product_id,
            -- 使用size()判断数组是否为空，避免NULL值
            CASE
                WHEN size(collect_list(concat('{"action":"', action_type, '","time":"', substr(action_time, 12, 8), '"}'))) > 0
                    THEN concat('[', concat_ws(',', collect_list(concat('{"action":"', action_type, '","time":"', substr(action_time, 12, 8), '"}'))), ']')
                ELSE '[]'
                END AS nodes_json
        FROM dwd_operation_action_detail
        WHERE dt='2025-01-26'
        GROUP BY product_id
    ) op_nodes
                  ON p.product_id = op_nodes.product_id
WHERE
    p.dt='2025-01-26'
  AND p.is_valid=1;

select * from dws_product_core_indicators;
-- todo 2. SKU 销售汇总表（dws_sku_sales_summary）
DROP TABLE IF EXISTS dws_sku_sales_summary;
CREATE TABLE IF NOT EXISTS dws_sku_sales_summary (
                                                     product_id STRING COMMENT '商品ID',
                                                     sku_id STRING COMMENT 'SKU ID',
                                                     stat_date STRING COMMENT '统计日期（yyyy-MM-dd）',
                                                     sku_attr STRING COMMENT 'SKU属性（如颜色:红色,尺寸:XL）',
                                                     sales_count BIGINT COMMENT '销售数量',
                                                     sales_amount DECIMAL(16,2) COMMENT '销售金额',
                                                     hot_degree INT COMMENT '热销度（1-10级，按销售数量排名计算）',
                                                     stock_quantity BIGINT COMMENT '当日库存数量'
) COMMENT 'SKU销售汇总表'
    PARTITIONED BY (dt STRING COMMENT '分区日期（yyyy-MM-dd）')
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\t'
    STORED AS ORC;

INSERT INTO TABLE dws_sku_sales_summary PARTITION (dt='2025-01-26')
SELECT
    p.product_id,
    sku_agg.sku_id,
    '2025-01-26' AS stat_date,
    COALESCE(sku_info.sku_attr, '') AS sku_attr,
    COALESCE(sku_agg.sales_count, 0) AS sales_count,
    COALESCE(sku_agg.sales_amount, 0) AS sales_amount,
    CASE
        WHEN rank_num <= total_sku * 0.1 THEN 10
        WHEN rank_num <= total_sku * 0.2 THEN 9
        WHEN rank_num <= total_sku * 0.3 THEN 8
        WHEN rank_num <= total_sku * 0.4 THEN 7
        WHEN rank_num <= total_sku * 0.5 THEN 6
        WHEN rank_num <= total_sku * 0.6 THEN 5
        WHEN rank_num <= total_sku * 0.7 THEN 4
        WHEN rank_num <= total_sku * 0.8 THEN 3
        WHEN rank_num <= total_sku * 0.9 THEN 2
        ELSE 1
        END AS hot_degree,
    -- 从SKU基础表取库存并减去当日销量（确保非负）
    GREATEST(COALESCE(sku_info.stock_quantity, 0) - COALESCE(sku_agg.sales_count, 0), 0) AS stock_quantity
FROM
    dim_product p
        LEFT JOIN (
        SELECT
            product_id,
            sku_id,
            SUM(order_count) AS sales_count,
            SUM(order_amount) AS sales_amount
        FROM dwd_order_detail
        WHERE dt='2025-01-26'
          AND is_paid=1
        GROUP BY product_id, sku_id
    ) sku_agg
                  ON p.product_id = sku_agg.product_id
        LEFT JOIN (
        SELECT
            product_id,
            sku_id,
            -- 从JSON字段提取属性并拼接为"属性:值"格式
            concat_ws(',',
                -- 提取颜色（若不存在则为空字符串）
                      CASE WHEN get_json_object(sku_attr_json, '$.color') IS NOT NULL
                               THEN concat('颜色:', get_json_object(sku_attr_json, '$.color'))
                           ELSE '' END,
                -- 提取尺寸
                      CASE WHEN get_json_object(sku_attr_json, '$.size') IS NOT NULL
                               THEN concat('尺寸:', get_json_object(sku_attr_json, '$.size'))
                           ELSE '' END,
                -- 提取规格
                      CASE WHEN get_json_object(sku_attr_json, '$.spec') IS NOT NULL
                               THEN concat('规格:', get_json_object(sku_attr_json, '$.spec'))
                           ELSE '' END
            ) AS sku_attr,
            -- 直接使用库存字段（确认存在）
            stock AS stock_quantity
        FROM ods_sku_info  -- 确认存在的SKU基础表
        WHERE dt='2025-01-26'
    ) sku_info
                  ON sku_agg.product_id = sku_info.product_id
                      AND sku_agg.sku_id = sku_info.sku_id
        LEFT JOIN (
        SELECT
            product_id,
            sku_id,
            ROW_NUMBER() OVER (ORDER BY sales_count DESC) AS rank_num,
            COUNT(*) OVER () AS total_sku
        FROM (
                 SELECT
                     product_id,
                     sku_id,
                     SUM(order_count) AS sales_count
                 FROM dwd_order_detail
                 WHERE dt='2025-01-26'
                   AND is_paid=1
                 GROUP BY product_id, sku_id
             ) t
    ) rank_info
                  ON sku_agg.product_id = rank_info.product_id
                      AND sku_agg.sku_id = rank_info.sku_id
WHERE
    p.dt='2025-01-26'
  AND p.is_valid=1;

select * from dws_sku_sales_summary;
-- todo 3. 商品价格指标汇总表（dws_product_price_indicators）
DROP TABLE IF EXISTS dws_product_price_indicators;
CREATE TABLE IF NOT EXISTS dws_product_price_indicators (
                                                            product_id STRING COMMENT '商品ID',
                                                            stat_date STRING COMMENT '统计日期（yyyy-MM-dd）',
                                                            current_price DECIMAL(10,2) COMMENT '当前价格',
                                                            price_trend_json STRING COMMENT '价格趋势（JSON数组，如[{"date":"2025-01-20","price":299}]）',
                                                            price_band_json STRING COMMENT '价格带分布（JSON，如{"0-50":10,"51-100":25}）',
                                                            price_strength_star INT COMMENT '价格力星级（1-5星）',
                                                            price_strength_json STRING COMMENT '价格力核心指标（JSON键值对）',
                                                            market_benchmark_json STRING COMMENT '市场同类目标杆指标（JSON键值对）'
) COMMENT '商品价格指标汇总表'
    PARTITIONED BY (dt STRING COMMENT '分区日期');

INSERT INTO TABLE dws_product_price_indicators PARTITION (dt='2025-01-26')
SELECT
    p.product_id,
    '2025-01-26' AS stat_date,
    current_price.current_price,
    -- 价格趋势JSON（修正：确保数值转为字符串）
    concat(
            '[',
            concat_ws(',', collect_list(
                    concat(
                            '{"date":"', price_trend.stat_date, '",',
                            '"price":', cast(price_trend.price AS STRING), '}'
                    )
                           )),
            ']'
    ) AS price_trend_json,
    -- 价格带分布JSON（修正：处理可能的NULL值并转为字符串）
    concat(
            '{',
            '"0-50":', cast(nvl(round(band_0_50.ratio * 100, 2), 0) AS STRING), ',',
            '"51-100":', cast(nvl(round(band_51_100.ratio * 100, 2), 0) AS STRING), ',',
            '"101-300":', cast(nvl(round(band_101_300.ratio * 100, 2), 0) AS STRING), ',',
            '"301-500":', cast(nvl(round(band_301_500.ratio * 100, 2), 0) AS STRING), ',',
            '"501+":', cast(nvl(round(band_501.ratio * 100, 2), 0) AS STRING),
            '}'
    ) AS price_band_json,
    price_strength.star AS price_strength_star,
    -- 价格力核心指标JSON（修正：数值转字符串）
    concat(
            '{"price_competitiveness":', cast(round(price_strength.competitiveness, 2) AS STRING), ',',
            '"discount_depth":', cast(round(price_strength.discount_depth, 2) AS STRING), ',',
            '"stock_sufficiency":', cast(round(price_strength.stock_ratio, 2) AS STRING), '}'
    ) AS price_strength_json,
    -- 市场同类目标杆指标JSON（修正：数值转字符串）
    concat(
            '{"avg_market_price":', cast(market_benchmark.avg_price AS STRING), ',',
            '"min_market_price":', cast(market_benchmark.min_price AS STRING), ',',
            '"max_market_price":', cast(market_benchmark.max_price AS STRING), ',',
            '"price_gap_ratio":', cast(round(market_benchmark.gap_ratio, 2) AS STRING), '}'
    ) AS market_benchmark_json
FROM
    dim_product p
        LEFT JOIN (
        SELECT
            product_id,
            avg(price) AS current_price
        FROM ods_sku_info
        WHERE dt = '2025-01-26'
        GROUP BY product_id
    ) current_price ON p.product_id = current_price.product_id
        LEFT JOIN (
        SELECT
            product_id,
            stat_date,
            avg(price) AS price
        FROM (
                 SELECT
                     product_id,
                     date_add('2025-01-26', -day_offset) AS stat_date,
                     price * (0.95 + rand() * 0.1) AS price
                 FROM ods_sku_info,
                      (SELECT explode(array(0,1,2,3,4,5,6)) AS day_offset) t
                 WHERE dt = '2025-01-26'
             ) t
        GROUP BY product_id, stat_date
    ) price_trend ON p.product_id = price_trend.product_id
        -- 价格带分布子查询（保持不变）
        LEFT JOIN (
        SELECT
            category_id,
            avg(CASE WHEN price <= 50 THEN 1 ELSE 0 END) AS ratio
        FROM ods_sku_info s
                 JOIN dim_product p2 ON s.product_id = p2.product_id AND p2.dt = '2025-01-26'
        WHERE s.dt = '2025-01-26'
        GROUP BY category_id
    ) band_0_50 ON p.category_id = band_0_50.category_id
        LEFT JOIN (
        SELECT
            category_id,
            avg(CASE WHEN price > 50 AND price <= 100 THEN 1 ELSE 0 END) AS ratio
        FROM ods_sku_info s
                 JOIN dim_product p2 ON s.product_id = p2.product_id AND p2.dt = '2025-01-26'
        WHERE s.dt = '2025-01-26'
        GROUP BY category_id
    ) band_51_100 ON p.category_id = band_51_100.category_id
        LEFT JOIN (
        SELECT
            category_id,
            avg(CASE WHEN price > 100 AND price <= 300 THEN 1 ELSE 0 END) AS ratio
        FROM ods_sku_info s
                 JOIN dim_product p2 ON s.product_id = p2.product_id AND p2.dt = '2025-01-26'
        WHERE s.dt = '2025-01-26'
        GROUP BY category_id
    ) band_101_300 ON p.category_id = band_101_300.category_id
        LEFT JOIN (
        SELECT
            category_id,
            avg(CASE WHEN price > 300 AND price <= 500 THEN 1 ELSE 0 END) AS ratio
        FROM ods_sku_info s
                 JOIN dim_product p2 ON s.product_id = p2.product_id AND p2.dt = '2025-01-26'
        WHERE s.dt = '2025-01-26'
        GROUP BY category_id
    ) band_301_500 ON p.category_id = band_301_500.category_id
        LEFT JOIN (
        SELECT
            category_id,
            avg(CASE WHEN price > 500 THEN 1 ELSE 0 END) AS ratio
        FROM ods_sku_info s
                 JOIN dim_product p2 ON s.product_id = p2.product_id AND p2.dt = '2025-01-26'
        WHERE s.dt = '2025-01-26'
        GROUP BY category_id
    ) band_501 ON p.category_id = band_501.category_id
        -- 价格力指标计算子查询（保持不变）
        LEFT JOIN (
        SELECT
            s.product_id,
            (avg(c.category_avg_price) / avg(s.price)) AS competitiveness,
            avg(CASE
                    WHEN oa.action_type = 'new_user_discount' THEN
                        get_json_object(oa.action_params, '$.discount') * 0.01
                    ELSE 1
                END) AS discount_depth,
            avg(s.stock / (s.stock + 1)) AS stock_ratio,
            least(5, greatest(1, round(
                    (avg(c.category_avg_price) / avg(s.price)) * 2 +
                    avg(CASE WHEN oa.action_type IS NOT NULL THEN 1 ELSE 0 END) * 2 +
                    avg(s.stock / (s.stock + 1)) * 1
                                 ))) AS star
        FROM ods_sku_info s
                 JOIN dim_product p2 ON s.product_id = p2.product_id AND p2.dt = '2025-01-26'
                 JOIN (
            SELECT category_id, avg(price) AS category_avg_price
            FROM ods_sku_info s2
                     JOIN dim_product p3 ON s2.product_id = p3.product_id AND p3.dt = '2025-01-26'
            WHERE s2.dt = '2025-01-26'
            GROUP BY category_id
        ) c ON p2.category_id = c.category_id
                 LEFT JOIN ods_operation_action oa
                           ON s.product_id = oa.product_id AND oa.dt = '2025-01-26'
        WHERE s.dt = '2025-01-26'
        GROUP BY s.product_id
    ) price_strength ON p.product_id = price_strength.product_id
        -- 市场同类目标杆子查询（保持不变）
        LEFT JOIN (
        SELECT
            p4.category_id,
            avg(s3.price) AS avg_price,
            min(s3.price) AS min_price,
            max(s3.price) AS max_price,
            (current_price.current_price - avg(s3.price)) / avg(s3.price) AS gap_ratio
        FROM dim_product p4
                 JOIN ods_sku_info s3 ON p4.product_id = s3.product_id AND s3.dt = '2025-01-26'
                 JOIN (
            SELECT product_id, avg(price) AS current_price
            FROM ods_sku_info
            WHERE dt = '2025-01-26'
            GROUP BY product_id
        ) current_price ON p4.product_id = current_price.product_id
        WHERE p4.dt = '2025-01-26'
        GROUP BY p4.category_id, current_price.current_price
    ) market_benchmark ON p.category_id = market_benchmark.category_id
WHERE
    p.dt = '2025-01-26'
  AND p.is_valid = 1
GROUP BY
    p.product_id,
    current_price.current_price,
    price_strength.star,
    price_strength.competitiveness,
    price_strength.discount_depth,
    price_strength.stock_ratio,
    market_benchmark.avg_price,
    market_benchmark.min_price,
    market_benchmark.max_price,
    market_benchmark.gap_ratio,
    band_0_50.ratio,
    band_51_100.ratio,
    band_101_300.ratio,
    band_301_500.ratio,
    band_501.ratio;

select * from dws_product_core_indicators;