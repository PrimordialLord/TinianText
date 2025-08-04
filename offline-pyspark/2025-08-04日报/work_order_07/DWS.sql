USE work_order_07;

-- 1. DWS 商品评分汇总（dws_product_score_summary）
DROP TABLE IF EXISTS dws_product_score_summary;
CREATE TABLE IF NOT EXISTS dws_product_score_summary
(
    product_id         STRING COMMENT '商品唯一标识',
    traffic_score      DOUBLE COMMENT '流量获取评分(0-100)',
    conversion_score   DOUBLE COMMENT '流量转化评分(0-100)',
    content_score      DOUBLE COMMENT '内容营销评分(0-100)',
    new_customer_score DOUBLE COMMENT '客户拉新评分(0-100)',
    service_score      DOUBLE COMMENT '服务质量评分(0-100)',
    total_score        DOUBLE COMMENT '综合评分(0-100)',
    score_level        STRING COMMENT '评分等级(A/B/C/D)',
    etl_time           STRING COMMENT 'ETL处理时间'
)
    COMMENT 'DWS层商品多维度评分汇总'
    PARTITIONED BY (dt STRING COMMENT '数据日期')
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\t'
    STORED AS PARQUET;

-- 从DWD层行为明细数据生成商品多维度评分汇总（修正版）
INSERT OVERWRITE TABLE dws_product_score_summary PARTITION (dt = '2025-08-03')
SELECT
    -- 商品唯一标识
    pb.product_id,

    -- 流量获取评分
    round(pb.traffic_index, 2)                             AS traffic_score,

    -- 流量转化评分
    round(
            (pb.conversion_rate / 100 * 50) + -- 转化率占比50%
            (pb.conversion_index * 0.5) -- 转化指标占比50%
        , 2)                                               AS conversion_score,

    -- 内容营销评分
    round(pb.content_index, 2)                             AS content_score,

    -- 客户拉新评分
    round(
            (greatest(log10(pb.new_customer_count + 1), 0) / 3 * 40) + -- 新增客户数占比40%
            (pb.new_customer_index * 0.6) -- 拉新指标占比60%
        , 2)                                               AS new_customer_score,

    -- 服务质量评分
    round(pb.service_index, 2)                             AS service_score,

    -- 综合评分（关键修正：用实际表达式替代别名）
    round(
        -- 流量占比20%（使用实际计算逻辑）
            (round(pb.traffic_index, 2) * 0.2) +
                -- 转化占比30%
            (round((pb.conversion_rate / 100 * 50) + (pb.conversion_index * 0.5), 2) * 0.3) +
                -- 内容占比10%
            (round(pb.content_index, 2) * 0.1) +
                -- 拉新占比20%
            (round(
                     (greatest(log10(pb.new_customer_count + 1), 0) / 3 * 40) + (pb.new_customer_index * 0.6), 2
             ) * 0.2) +
                -- 服务占比20%
            (round(pb.service_index, 2) * 0.2)
        , 2)                                               AS total_score,

    -- 评分等级（根据综合评分划分）
    case
        when round(
                     (round(pb.traffic_index, 2) * 0.2) +
                     (round((pb.conversion_rate / 100 * 50) + (pb.conversion_index * 0.5), 2) * 0.3) +
                     (round(pb.content_index, 2) * 0.1) +
                     (round(
                              (greatest(log10(pb.new_customer_count + 1), 0) / 3 * 40) + (pb.new_customer_index * 0.6),
                              2
                      ) * 0.2) +
                     (round(pb.service_index, 2) * 0.2)
                 , 2) >= 90 then 'A'
        when round(
                     (round(pb.traffic_index, 2) * 0.2) +
                     (round((pb.conversion_rate / 100 * 50) + (pb.conversion_index * 0.5), 2) * 0.3) +
                     (round(pb.content_index, 2) * 0.1) +
                     (round(
                              (greatest(log10(pb.new_customer_count + 1), 0) / 3 * 40) + (pb.new_customer_index * 0.6),
                              2
                      ) * 0.2) +
                     (round(pb.service_index, 2) * 0.2)
                 , 2) >= 80 then 'B'
        when round(
                     (round(pb.traffic_index, 2) * 0.2) +
                     (round((pb.conversion_rate / 100 * 50) + (pb.conversion_index * 0.5), 2) * 0.3) +
                     (round(pb.content_index, 2) * 0.1) +
                     (round(
                              (greatest(log10(pb.new_customer_count + 1), 0) / 3 * 40) + (pb.new_customer_index * 0.6),
                              2
                      ) * 0.2) +
                     (round(pb.service_index, 2) * 0.2)
                 , 2) >= 60 then 'C'
        else 'D'
        end                                                AS score_level,

    -- ETL处理时间
    from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss') AS etl_time
FROM dwd_product_base_detail pb_base
         JOIN dwd_product_behavior_detail pb
              ON pb_base.product_id = pb.product_id
                  AND pb_base.dt = pb.dt
WHERE pb_base.dt = '2025-08-03'
  AND pb.traffic_index IS NOT NULL
  AND pb.service_index IS NOT NULL;

select *
from dws_product_score_summary;


-- 2. DWS 商品运营指标汇总（dws_product_operation_summary）
DROP TABLE IF EXISTS dws_product_operation_summary;
CREATE TABLE IF NOT EXISTS dws_product_operation_summary
(
    product_id          STRING COMMENT '商品ID',
    visitor_count       BIGINT COMMENT '访客数',
    sales_amount        DOUBLE COMMENT '销售金额',
    sales_volume        BIGINT COMMENT '销量',
    conversion_rate     DOUBLE COMMENT '转化率(%)',
    new_customer_count  BIGINT COMMENT '新增客户数',
    price_sales_ratio   DOUBLE COMMENT '价格-销量比',
    visitor_sales_ratio DOUBLE COMMENT '访客-销量比',
    etl_time            STRING COMMENT 'ETL处理时间'
)
    COMMENT 'DWS层商品运营指标汇总'
    PARTITIONED BY (dt STRING COMMENT '数据日期')
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\t'
    STORED AS PARQUET;

select *
from dwd_product_behavior_detail;
-- 从DWD层数据生成商品运营指标汇总
INSERT OVERWRITE TABLE dws_product_operation_summary PARTITION (dt = '2025-08-03')
SELECT
    -- 商品唯一标识（关联基础信息与行为数据）
    pb.product_id,

    -- 访客数（直接取自行为明细）
    pb.visitor_count,

    -- 销售金额（保留两位小数）
    round(pb.sales_amount, 2)                              AS sales_amount,

    -- 销量（直接取自行为明细）
    pb.sales_volume,

    -- 转化率（保留两位小数）
    round(pb.conversion_rate, 2)                           AS conversion_rate,

    -- 新增客户数（直接取自行为明细）
    pb.new_customer_count,

    -- 价格-销量比（反映价格对销量的影响，销量/价格，保留4位小数）
    round(
            case when pb_base.price > 0 then pb.sales_volume / pb_base.price else 0 end, 4
    )                                                      AS price_sales_ratio,

    -- 访客-销量比（反映流量转化效率，销量/访客数，保留4位小数）
    round(
            case when pb.visitor_count > 0 then pb.sales_volume / pb.visitor_count else 0 end, 4
    )                                                      AS visitor_sales_ratio,

    -- ETL处理时间（当前时间）
    from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss') AS etl_time
FROM
    -- 主表：商品行为明细表（核心运营指标来源）
    dwd_product_behavior_detail pb
-- 关联商品基础表（获取商品价格用于计算比率）
        JOIN dwd_product_base_detail pb_base
             ON pb.product_id = pb_base.product_id
                 AND pb.dt = pb_base.dt -- 确保时间分区一致
WHERE pb.dt = '2025-08-03' -- 过滤当前分区数据
  -- 过滤关键指标异常值
  AND pb.visitor_count >= 0
  AND pb.sales_volume >= 0; -- 排除价格为0的无效商品

INSERT OVERWRITE TABLE dws_product_operation_summary PARTITION (dt = '2025-08-03')
SELECT
    -- 商品ID（行为表为主，避免因关联失败丢失数据）
    pb.product_id,

    -- 访客数（保留原始值，无异常过滤）
    pb.visitor_count,

    -- 销售金额（保留两位小数）
    round(pb.sales_amount, 2)                              AS sales_amount,

    -- 销量（保留原始值）
    pb.sales_volume,

    -- 转化率（保留两位小数）
    round(pb.conversion_rate, 2)                           AS conversion_rate,

    -- 新增客户数
    pb.new_customer_count,

    -- 价格-销量比（兼容无基础表数据的情况）
    round(
            case
                when pb_base.price > 0 then pb.sales_volume / pb_base.price
                else 0 -- 无价格数据时默认0
                end, 4
    )                                                      AS price_sales_ratio,

    -- 访客-销量比（兼容访客数为0的情况）
    round(
            case when pb.visitor_count > 0 then pb.sales_volume / pb.visitor_count else 0 end, 4
    )                                                      AS visitor_sales_ratio,

    -- ETL时间
    from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss') AS etl_time
FROM
    -- 主表：行为表（保留所有数据）
    dwd_product_behavior_detail pb
-- 左关联基础表（避免因关联失败丢失行为数据）
        LEFT JOIN dwd_product_base_detail pb_base
                  ON pb.product_id = pb_base.product_id
                      AND pb.dt = pb_base.dt
WHERE pb.dt = '2025-08-03'; -- 仅保留日期过滤，放宽其他条件

select *
from dws_product_operation_summary;


