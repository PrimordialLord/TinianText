USE work_order_07;
-- ADS 全品价值评估（ads_product_value_assessment
DROP TABLE IF EXISTS ads_product_value_assessment;
CREATE TABLE IF NOT EXISTS ads_product_value_assessment
(
    product_id          STRING COMMENT '商品ID',
    product_name        STRING COMMENT '商品名称',
    category_name       STRING COMMENT '分类名称',
    total_score         DOUBLE COMMENT '综合评分',
    score_level         STRING COMMENT '评分等级',
    price_sales_ratio   DOUBLE COMMENT '价格-销量比',
    visitor_sales_ratio DOUBLE COMMENT '访客-销量比',
    sales_amount        DOUBLE COMMENT '销售金额',
    sales_volume        BIGINT COMMENT '销量',
    stat_date           STRING COMMENT '统计日期'
)
    COMMENT 'ADS层全品价值评估数据'
    PARTITIONED BY (dt STRING COMMENT '数据日期')
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\t'
    STORED AS PARQUET;

INSERT OVERWRITE TABLE ads_product_value_assessment PARTITION (dt = '2025-08-03')
SELECT
    -- 商品ID（以运营表为准，确保非空）
    pos.product_id,

    -- 商品名称（兼容基础表无数据的情况）
    COALESCE(pbd.product_name, '未知商品'),

    -- 分类名称（兼容基础表无数据的情况）
    COALESCE(pbd.category_name, '未知分类'),

    -- 综合评分（兼容评分表无数据的情况）
    COALESCE(dpss.total_score, 0.0),

    -- 评分等级（兼容评分表无数据的情况）
    COALESCE(dpss.score_level, '未评级'),

    -- 价格-销量比（直接取自运营表）
    pos.price_sales_ratio,

    -- 访客-销量比（直接取自运营表）
    pos.visitor_sales_ratio,

    -- 销售金额（直接取自运营表）
    pos.sales_amount,

    -- 销量（直接取自运营表）
    pos.sales_volume,

    -- 统计日期
    '2025-08-03' AS stat_date
FROM
    -- 主表：运营指标汇总表（保留所有数据）
    dws_product_operation_summary pos
-- 左关联评分表（允许无评分数据）
        LEFT JOIN dws_product_score_summary dpss
                  ON pos.product_id = dpss.product_id
                      AND pos.dt = dpss.dt
-- 左关联基础表（允许无基础信息）
        LEFT JOIN dwd_product_base_detail pbd
                  ON pos.product_id = pbd.product_id
                      AND pos.dt = pbd.dt
WHERE pos.dt = '2025-08-03' -- 仅保留日期过滤
  AND pos.product_id IS NOT NULL; -- 仅过滤无效商品ID

select *
from ads_product_value_assessment;

-- ADS 单品竞争力诊断（ads_product_competitor_diagnosis）
DROP TABLE IF EXISTS ads_product_competitor_diagnosis;
CREATE TABLE IF NOT EXISTS ads_product_competitor_diagnosis
(
    product_id              STRING COMMENT '本品ID',
    product_name            STRING COMMENT '本品名称',
    competitor_product_id   STRING COMMENT '竞品ID',
    dimension               STRING COMMENT '对比维度',
    self_value              DOUBLE COMMENT '本品指标值',
    competitor_avg_value    DOUBLE COMMENT '竞品平均值',
    gap_ratio               DOUBLE COMMENT '差距比例(%)',
    optimization_suggestion STRING COMMENT '优化建议',
    stat_date               STRING COMMENT '统计日期'
)
    COMMENT 'ADS层单品竞争力诊断数据'
    PARTITIONED BY (dt STRING COMMENT '数据日期')
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\t'
    STORED AS PARQUET;

-- 生成ADS层单品竞争力诊断数据（修正版）
INSERT OVERWRITE TABLE ads_product_competitor_diagnosis PARTITION (dt = '2025-08-03')
SELECT cd.product_id,
       COALESCE(pb.product_name, '未知商品')                               AS product_name,
       cd.competitor_product_id,
       cd.dimension,
       cd.self_value,
       -- 同维度竞品平均值
       round(AVG(cd.competitor_value) OVER (PARTITION BY cd.dimension), 2) AS competitor_avg_value,
       -- 差距比例（保留表达式用于后续判断）
       round(
               CASE
                   WHEN AVG(cd.competitor_value) OVER (PARTITION BY cd.dimension) > 0
                       THEN (AVG(cd.competitor_value) OVER (PARTITION BY cd.dimension) - cd.self_value)
                                / AVG(cd.competitor_value) OVER (PARTITION BY cd.dimension) * 100
                   ELSE 0
                   END, 2
       )                                                                   AS gap_ratio,
       -- 优化建议（用实际计算表达式替代gap_ratio别名）
       CASE
           WHEN round(
                        CASE
                            WHEN AVG(cd.competitor_value) OVER (PARTITION BY cd.dimension) > 0
                                THEN (AVG(cd.competitor_value) OVER (PARTITION BY cd.dimension) - cd.self_value)
                                         / AVG(cd.competitor_value) OVER (PARTITION BY cd.dimension) * 100
                            ELSE 0
                            END, 2
                ) >= 30 THEN CONCAT('【紧急优化】本品在"', cd.dimension, '"维度落后竞品30%以上，需优先改进')

           WHEN round(
                        CASE
                            WHEN AVG(cd.competitor_value) OVER (PARTITION BY cd.dimension) > 0
                                THEN (AVG(cd.competitor_value) OVER (PARTITION BY cd.dimension) - cd.self_value)
                                         / AVG(cd.competitor_value) OVER (PARTITION BY cd.dimension) * 100
                            ELSE 0
                            END, 2
                ) >= 10 THEN CONCAT('【重点优化】本品在"', cd.dimension, '"维度落后竞品10%-30%，建议针对性提升')

           WHEN round(
                        CASE
                            WHEN AVG(cd.competitor_value) OVER (PARTITION BY cd.dimension) > 0
                                THEN (AVG(cd.competitor_value) OVER (PARTITION BY cd.dimension) - cd.self_value)
                                         / AVG(cd.competitor_value) OVER (PARTITION BY cd.dimension) * 100
                            ELSE 0
                            END, 2
                ) > 0 THEN CONCAT('【轻微优化】本品在"', cd.dimension, '"维度落后竞品0-10%，可微调优化')

           WHEN round(
                        CASE
                            WHEN AVG(cd.competitor_value) OVER (PARTITION BY cd.dimension) > 0
                                THEN (AVG(cd.competitor_value) OVER (PARTITION BY cd.dimension) - cd.self_value)
                                         / AVG(cd.competitor_value) OVER (PARTITION BY cd.dimension) * 100
                            ELSE 0
                            END, 2
                ) <= 0 THEN CONCAT('【保持优势】本品在"', cd.dimension, '"维度优于竞品平均水平，建议维持')

           ELSE '【数据异常】无法计算差距，需检查原始数据'
           END                                                             AS optimization_suggestion,
       '2025-08-03'                                                        AS stat_date
FROM dwd_competitor_detail cd
         LEFT JOIN dwd_product_base_detail pb
                   ON cd.product_id = pb.product_id
                       AND cd.dt = pb.dt
WHERE cd.dt = '2025-08-03'
  AND cd.self_value IS NOT NULL
  AND cd.competitor_value IS NOT NULL;

select *
from ads_product_competitor_diagnosis;



