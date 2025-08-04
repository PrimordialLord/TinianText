USE work_order_08;


-- 工具效果总览表
DROP TABLE IF EXISTS ads_tool_effect_overview;
CREATE TABLE IF NOT EXISTS ads_tool_effect_overview
(
    statistics_period          STRING COMMENT '统计周期：日/7天/30天',
    start_date                 DATE COMMENT '统计开始日期',
    end_date                   DATE COMMENT '统计结束日期',
    total_send_count           BIGINT COMMENT '总发送次数',
    total_pay_count            BIGINT COMMENT '总支付次数',
    total_pay_amount           DECIMAL(12, 2) COMMENT '总支付金额',
    avg_discount               DECIMAL(10, 2) COMMENT '平均优惠金额',
    activity_in_progress_count INT COMMENT '进行中活动数',
    activity_ended_count       INT COMMENT '已结束活动数',
    create_time                TIMESTAMP COMMENT '数据创建时间'
) COMMENT '工具效果总览表'
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\t'
    STORED AS ORC;

-- 插入数据到工具效果总览表（优化版）
INSERT OVERWRITE TABLE ads_tool_effect_overview
SELECT
    -- 统计周期（日/7天/30天）
    stats.period                                                                                                     AS statistics_period,
    -- 统计开始日期
    stats.start_date,
    -- 统计结束日期
    stats.end_date,
    -- 总发送次数：兼容空数据（无数据时为0）
    COALESCE(SUM(effect.send_count), 0)                                                                              AS total_send_count,
    -- 总支付次数：兼容空数据
    COALESCE(SUM(effect.pay_count), 0)                                                                               AS total_pay_count,
    -- 总支付金额：兼容空数据
    COALESCE(SUM(effect.total_pay_amount), 0.00)                                                                     AS total_pay_amount,
    -- 平均优惠金额：处理除数为0和空值
    CASE
        WHEN COALESCE(SUM(effect.pay_count), 0) = 0 THEN 0.00
        ELSE ROUND(COALESCE(SUM(effect.total_pay_amount), 0.00) / SUM(effect.pay_count), 2)
        END                                                                                                          AS avg_discount,
    -- 进行中活动数：直接从活动表统计，不依赖效果表
    COUNT(DISTINCT CASE
                       WHEN act.status = '进行中' AND
                            DATE(act.start_time) <= stats.end_date AND
                            DATE(act.end_time) >= stats.start_date
                           THEN act.activity_id END)                                                                 AS activity_in_progress_count,
    -- 已结束活动数：直接从活动表统计
    COUNT(DISTINCT CASE
                       WHEN act.status = '已结束' AND
                            DATE(act.end_time) BETWEEN stats.start_date AND stats.end_date
                           THEN act.activity_id END)                                                                 AS activity_ended_count,
    -- 数据创建时间
    CURRENT_TIMESTAMP()                                                                                              AS create_time
FROM
    -- 生成独立的统计周期（即使无效果数据也会保留周期记录）
    (
        -- 1. 日周期：近30天的每一天
        SELECT '日'                                 AS period,
               DATE(DATE_ADD(CURRENT_DATE(), -day)) AS start_date,
               DATE(DATE_ADD(CURRENT_DATE(), -day)) AS end_date
        FROM (SELECT pos AS day
              FROM (SELECT posexplode(split(space(29), '')) AS (pos, val)) t) num
        UNION ALL
        -- 2. 7天周期：近7天内的连续7天范围
        SELECT '7天'                                    AS period,
               DATE(DATE_ADD(CURRENT_DATE(), -day - 6)) AS start_date,
               DATE(DATE_ADD(CURRENT_DATE(), -day))     AS end_date
        FROM (SELECT pos AS day
              FROM (SELECT posexplode(split(space(6), '')) AS (pos, val)) t) num
        UNION ALL
        -- 3. 30天周期：固定最近30天
        SELECT '30天'                              AS period,
               DATE(DATE_ADD(CURRENT_DATE(), -29)) AS start_date,
               CURRENT_DATE()                      AS end_date) stats
-- 左关联活动效果表（允许无效果数据的周期保留）
        LEFT JOIN work_order_08.dws_activity_effect_summary effect
                  ON effect.statistics_period = stats.period
                      AND effect.start_date = stats.start_date
                      AND effect.end_date = stats.end_date
-- 左关联活动表（跨库关联，确保活动数据可访问）
        LEFT JOIN work_order_08.dwd_activity_info act ON 1 = 1
-- 按统计周期分组（确保每个周期都有记录）
GROUP BY stats.period,
         stats.start_date,
         stats.end_date;

select *
from ads_tool_effect_overview;

-- 活动详情表
CREATE TABLE IF NOT EXISTS ads_activity_detail
(
    activity_id     BIGINT COMMENT '活动编号',
    activity_name   STRING COMMENT '活动名称',
    activity_level  STRING COMMENT '活动级别',
    activity_type   STRING COMMENT '优惠类型',
    start_time      TIMESTAMP COMMENT '活动开始时间',
    end_time        TIMESTAMP COMMENT '活动结束时间',
    send_count      BIGINT COMMENT '发送次数',
    pay_count       BIGINT COMMENT '支付次数',
    pay_amount      DECIMAL(12, 2) COMMENT '支付金额',
    use_rate        DECIMAL(5, 2) COMMENT '使用率',
    top3_product_id ARRAY<BIGINT> COMMENT '发送次数前三的商品ID',
    create_time     TIMESTAMP COMMENT '数据创建时间'
) COMMENT '活动详情表'
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\t'
        COLLECTION ITEMS TERMINATED BY ','
    STORED AS ORC;


INSERT OVERWRITE TABLE ads_activity_detail
SELECT
    -- 活动基础信息
    act.activity_id,
    act.activity_name,
    act.activity_level,
    act.activity_type,
    act.start_time,
    act.end_time,
    -- 活动效果指标
    COALESCE(effect.total_send, 0)      AS send_count,
    COALESCE(effect.total_pay, 0)       AS pay_count,
    COALESCE(effect.total_amount, 0.00) AS pay_amount,
    -- 使用率计算
    CASE
        WHEN COALESCE(effect.total_send, 0) = 0 THEN 0.00
        ELSE ROUND(effect.total_pay / effect.total_send * 100, 2)
        END                             AS use_rate,
    -- 发送次数前三的商品ID（确保array<bigint>类型一致）
    CASE
        WHEN product_rank.top3_products IS NULL THEN array(cast(NULL AS BIGINT)) -- 显式bigint空数组
        ELSE product_rank.top3_products
        END                             AS top3_product_id,
    -- 数据创建时间
    CURRENT_TIMESTAMP()                 AS create_time
FROM work_order_08.dwd_activity_info act
         LEFT JOIN (SELECT activity_id,
                           SUM(send_count)       AS total_send,
                           SUM(pay_count)        AS total_pay,
                           SUM(total_pay_amount) AS total_amount
                    FROM work_order_08.dws_activity_effect_summary
                    GROUP BY activity_id) effect ON act.activity_id = effect.activity_id
         LEFT JOIN (SELECT activity_id,
                           -- 强制生成array<bigint>
                           collect_list(cast(t.product_id AS BIGINT)) AS top3_products
                    FROM (SELECT send.activity_id,
                                 send.product_id,
                                 COUNT(DISTINCT send.id) AS product_send_count,
                                 ROW_NUMBER() OVER (
                                     PARTITION BY send.activity_id
                                     ORDER BY COUNT(DISTINCT send.id) DESC
                                     )                   AS rnk
                          FROM work_order_08.dwd_customer_service_send_detail send
                          WHERE send.product_id IS NOT NULL
                          GROUP BY send.activity_id, send.product_id) t
                    WHERE rnk <= 3
                    GROUP BY activity_id) product_rank ON act.activity_id = product_rank.activity_id;

select *
from ads_activity_detail;

-- 客服排名表
CREATE TABLE IF NOT EXISTS ads_customer_service_rank
(
    customer_service_id   BIGINT COMMENT '客服ID',
    customer_service_name STRING COMMENT '客服姓名',
    statistics_period     STRING COMMENT '统计周期：日/7天/30天',
    send_count            BIGINT COMMENT '发送次数',
    pay_count             BIGINT COMMENT '支付次数',
    conversion_rate       DECIMAL(5, 2) COMMENT '转化率（支付次数/发送次数）',
    rank                  INT COMMENT '排名',
    create_time           TIMESTAMP COMMENT '数据创建时间'
) COMMENT '客服排名表'
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\t'
    STORED AS ORC;

-- 客服排名表数据生成
INSERT OVERWRITE TABLE ads_customer_service_rank
SELECT
    -- 客服基础信息
    perf.customer_service_id,
    perf.customer_service_name,
    -- 统计周期
    perf.statistics_period,
    -- 发送次数
    perf.send_count,
    -- 支付次数
    perf.successful_pay_count AS pay_count,
    -- 转化率（支付次数/发送次数，保留2位小数）
    CASE
        WHEN perf.send_count = 0 THEN 0.00
        ELSE ROUND(perf.successful_pay_count * 100.0 / perf.send_count, 2)
        END                   AS conversion_rate,
    -- 排名（按转化率降序，发送次数降序）
    ROW_NUMBER() OVER (
        PARTITION BY perf.statistics_period
        ORDER BY
            CASE WHEN perf.send_count = 0 THEN 0 ELSE perf.successful_pay_count * 100.0 / perf.send_count END DESC,
            perf.send_count DESC
        )                     AS rank,
    -- 数据创建时间
    CURRENT_TIMESTAMP()       AS create_time
FROM
    -- 数据源：客服绩效汇总表
    work_order_08.dws_customer_service_performance perf;


select *
from ads_customer_service_rank;
-- 商品效果表
CREATE TABLE IF NOT EXISTS ads_product_effect
(
    product_id          BIGINT COMMENT '商品ID',
    product_name        STRING COMMENT '商品名称',
    activity_id         BIGINT COMMENT '活动编号',
    activity_name       STRING COMMENT '活动名称',
    send_count          BIGINT COMMENT '发送次数',
    pay_count           BIGINT COMMENT '支付次数',
    discount_amount_avg DECIMAL(10, 2) COMMENT '平均优惠金额',
    create_time         TIMESTAMP COMMENT '数据创建时间'
) COMMENT '商品效果表'
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\t'
    STORED AS ORC;


-- 商品效果表数据生成
INSERT OVERWRITE TABLE ads_product_effect
SELECT
    -- 商品ID
    p.product_id,
    -- 商品名称
    p.product_name,
    -- 活动编号（可能为NULL，代表非活动商品）
    act.activity_id,
    -- 活动名称（非活动商品对应NULL）
    act.activity_name,
    -- 发送次数：该商品在活动中的总发送次数
    COALESCE(SUM(send.total_send), 0) AS send_count,
    -- 支付次数：该商品在活动中被使用的支付次数
    COALESCE(SUM(send.total_pay), 0)  AS pay_count,
    -- 平均优惠金额：使用优惠的平均金额
    CASE
        WHEN COALESCE(SUM(send.total_pay), 0) = 0 THEN 0.00
        ELSE ROUND(SUM(send.total_discount) / SUM(send.total_pay), 2)
        END                           AS discount_amount_avg,
    -- 数据创建时间
    CURRENT_TIMESTAMP()               AS create_time
FROM
    -- 主表：商品明细信息
    work_order_08.dwd_product_info p
-- 关联商品活动关联表（获取商品参与的活动）
        LEFT JOIN work_order_08.dwd_product_activity_rel rel
                  ON p.product_id = rel.product_id
-- 关联活动信息表（获取活动名称）
        LEFT JOIN work_order_08.dwd_activity_info act
                  ON rel.activity_id = act.activity_id
-- 关联商品优惠发送汇总数据（按商品+活动分组）
        LEFT JOIN (SELECT product_id,
                          activity_id,
                          -- 该商品在活动中的总发送次数
                          COUNT(DISTINCT id)                                       AS total_send,
                          -- 该商品在活动中的总支付次数
                          COUNT(DISTINCT CASE WHEN is_used = 1 THEN id END)        AS total_pay,
                          -- 该商品在活动中被使用的优惠总金额
                          SUM(CASE WHEN is_used = 1 THEN send_discount ELSE 0 END) AS total_discount
                   FROM work_order_08.dwd_customer_service_send_detail
                   GROUP BY product_id, activity_id) send
                  ON p.product_id = send.product_id
                      AND rel.activity_id = send.activity_id
-- 按商品和活动分组（确保每个商品在每个活动中生成一条记录）
GROUP BY p.product_id,
         p.product_name,
         act.activity_id,
         act.activity_name;

select *
from ads_product_effect;