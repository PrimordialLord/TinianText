USE work_order_08;

-- 活动效果汇总表
CREATE TABLE IF NOT EXISTS dws_activity_effect_summary
(
    activity_id       BIGINT COMMENT '活动编号',
    activity_name     STRING COMMENT '活动名称',
    statistics_period STRING COMMENT '统计周期：日/7天/30天',
    start_date        DATE COMMENT '统计开始日期',
    end_date          DATE COMMENT '统计结束日期',
    send_count        BIGINT COMMENT '发送次数',
    pay_count         BIGINT COMMENT '支付次数',
    total_pay_amount  DECIMAL(12, 2) COMMENT '总支付金额',
    use_rate          DECIMAL(5, 2) COMMENT '优惠使用率（使用次数/发送次数）',
    create_time       TIMESTAMP COMMENT '数据创建时间'
) COMMENT '活动效果汇总表'
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\t'
    STORED AS ORC;

-- 活动效果汇总表数据生成
INSERT OVERWRITE TABLE dws_activity_effect_summary
SELECT
    -- 活动编号
    act.activity_id,
    -- 活动名称
    act.activity_name,
    -- 统计周期（通过条件判断当前汇总的周期类型）
    stats.period                                                       AS statistics_period,
    -- 统计开始日期
    stats.start_date,
    -- 统计结束日期
    stats.end_date,
    -- 发送次数：该周期内客服发送的优惠次数
    COUNT(DISTINCT send.id)                                            AS send_count,
    -- 支付次数：该周期内使用优惠完成支付的次数（假设use_time对应支付时间）
    COUNT(DISTINCT CASE WHEN send.is_used = 1 THEN send.id END)        AS pay_count,
    -- 总支付金额：需关联订单表，此处用“优惠金额*使用次数”模拟（实际应关联订单金额）
    SUM(CASE WHEN send.is_used = 1 THEN send.send_discount ELSE 0 END) AS total_pay_amount,
    -- 优惠使用率：使用次数/发送次数（避免除数为0）
    CASE
        WHEN COUNT(DISTINCT send.id) = 0 THEN 0
        ELSE ROUND(
                COUNT(DISTINCT CASE WHEN send.is_used = 1 THEN send.id END)
                    / COUNT(DISTINCT send.id) * 100,
                2
             )
        END                                                            AS use_rate,
    -- 数据创建时间（当前时间）
    CURRENT_TIMESTAMP()                                                AS create_time
FROM
    -- 主表：活动明细事实表（获取活动基础信息）
    dwd_activity_info act
-- 关联统计周期维度（通过子查询生成周期范围）
        CROSS JOIN (
        -- 生成日/7天/30天三种周期的统计范围（以活动结束时间为基准，实际可按业务需求调整）
        SELECT '日'               AS period,
               DATE(act.end_time) AS start_date,
               DATE(act.end_time) AS end_date
        FROM dwd_activity_info act
        UNION ALL
        SELECT '7天'                            AS period,
               DATE(DATE_ADD(act.end_time, -6)) AS start_date, -- 结束时间前6天（共7天）
               DATE(act.end_time)               AS end_date
        FROM dwd_activity_info act
        UNION ALL
        SELECT '30天'                            AS period,
               DATE(DATE_ADD(act.end_time, -29)) AS start_date, -- 结束时间前29天（共30天）
               DATE(act.end_time)                AS end_date
        FROM dwd_activity_info act) stats
-- 关联客服发送优惠明细（获取发送和使用数据）
        LEFT JOIN dwd_customer_service_send_detail send
                  ON act.activity_id = send.activity_id
                      AND DATE(send.send_time) BETWEEN stats.start_date AND stats.end_date -- 发送时间在统计周期内
-- 过滤有效活动
WHERE act.activity_id IS NOT NULL
-- 按活动和统计周期分组
GROUP BY act.activity_id,
         act.activity_name,
         stats.period,
         stats.start_date,
         stats.end_date;

select *
from dws_activity_effect_summary;

-- 客服绩效汇总表
CREATE TABLE IF NOT EXISTS dws_customer_service_performance
(
    customer_service_id   BIGINT COMMENT '客服ID',
    customer_service_name STRING COMMENT '客服姓名',
    statistics_period     STRING COMMENT '统计周期：日/7天/30天',
    start_date            DATE COMMENT '统计开始日期',
    end_date              DATE COMMENT '统计结束日期',
    send_count            BIGINT COMMENT '发送次数',
    successful_pay_count  BIGINT COMMENT '成功支付次数',
    contribution_amount   DECIMAL(12, 2) COMMENT '贡献金额',
    create_time           TIMESTAMP COMMENT '数据创建时间'
) COMMENT '客服绩效汇总表'
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\t'
    STORED AS ORC;

-- 客服绩效汇总表数据生成（兼容低版本Hive）
INSERT OVERWRITE TABLE dws_customer_service_performance
SELECT
    -- 客服ID
    send.customer_service_id,
    -- 客服姓名（用ID拼接默认名称）
    CONCAT('客服_', send.customer_service_id)                          AS customer_service_name,
    -- 统计周期（日/7天/30天）
    stats.period                                                       AS statistics_period,
    -- 统计开始日期
    stats.start_date,
    -- 统计结束日期
    stats.end_date,
    -- 发送次数：周期内客服发送的优惠总次数
    COUNT(DISTINCT send.id)                                            AS send_count,
    -- 成功支付次数：周期内发送的优惠被使用并完成支付的次数
    COUNT(DISTINCT CASE WHEN send.is_used = 1 THEN send.id END)        AS successful_pay_count,
    -- 贡献金额：使用优惠产生的支付金额
    SUM(CASE WHEN send.is_used = 1 THEN send.send_discount ELSE 0 END) AS contribution_amount,
    -- 数据创建时间（当前时间）
    CURRENT_TIMESTAMP()                                                AS create_time
FROM
    -- 主表：客服发送优惠明细事实表
    dwd_customer_service_send_detail send
-- 关联统计周期维度（兼容低版本Hive的周期生成方式）
        CROSS JOIN (
        -- 1. 日周期：近30天的每一天
        SELECT '日'                                 AS period,
               DATE(DATE_ADD(CURRENT_DATE(), -day)) AS start_date,
               DATE(DATE_ADD(CURRENT_DATE(), -day)) AS end_date
        FROM (
                 -- 生成0-29的数字序列（代表近30天）
                 SELECT pos AS day
                 FROM (SELECT posexplode(split(space(29), '')) AS (pos, val)) t) num
        UNION ALL
        -- 2. 7天周期：近7天内，以每天为结束日的连续7天
        SELECT '7天'                                    AS period,
               DATE(DATE_ADD(CURRENT_DATE(), -day - 6)) AS start_date, -- 开始日期=结束日期-6天
               DATE(DATE_ADD(CURRENT_DATE(), -day))     AS end_date
        FROM (
                 -- 生成0-6的数字序列（代表近7天）
                 SELECT pos AS day
                 FROM (SELECT posexplode(split(space(6), '')) AS (pos, val)) t) num
        UNION ALL
        -- 3. 30天周期：固定为最近30天
        SELECT '30天'                              AS period,
               DATE(DATE_ADD(CURRENT_DATE(), -29)) AS start_date, -- 30天前
               CURRENT_DATE()                      AS end_date) stats
-- 过滤条件：发送时间在统计周期内
WHERE DATE(send.send_time) BETWEEN stats.start_date AND stats.end_date
-- 按客服ID和统计周期分组
GROUP BY send.customer_service_id,
         stats.period,
         stats.start_date,
         stats.end_date;

select *
from dws_customer_service_performance;

-- 商品优惠汇总表
CREATE TABLE IF NOT EXISTS dws_product_discount_summary
(
    product_id        BIGINT COMMENT '商品ID',
    product_name      STRING COMMENT '商品名称',
    statistics_period STRING COMMENT '统计周期：日/7天/30天',
    start_date        DATE COMMENT '统计开始日期',
    end_date          DATE COMMENT '统计结束日期',
    activity_count    INT COMMENT '参与活动数',
    total_send_count  BIGINT COMMENT '总发送次数',
    total_pay_count   BIGINT COMMENT '总支付次数',
    create_time       TIMESTAMP COMMENT '数据创建时间'
) COMMENT '商品优惠汇总表'
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\t'
    STORED AS ORC;

-- 商品优惠汇总表数据生成
INSERT OVERWRITE TABLE dws_product_discount_summary
SELECT
    -- 商品ID
    p.product_id,
    -- 商品名称
    p.product_name,
    -- 统计周期（日/7天/30天）
    stats.period                                                AS statistics_period,
    -- 统计开始日期
    stats.start_date,
    -- 统计结束日期
    stats.end_date,
    -- 参与活动数：周期内商品参与的 distinct 活动数量
    COUNT(DISTINCT rel.activity_id)                             AS activity_count,
    -- 总发送次数：周期内该商品相关优惠的发送总次数
    COUNT(DISTINCT send.id)                                     AS total_send_count,
    -- 总支付次数：周期内该商品优惠被使用的支付次数
    COUNT(DISTINCT CASE WHEN send.is_used = 1 THEN send.id END) AS total_pay_count,
    -- 数据创建时间
    CURRENT_TIMESTAMP()                                         AS create_time
FROM
    -- 主表：商品明细事实表
    dwd_product_info p
-- 关联商品活动关联表（获取商品参与的活动）
        LEFT JOIN dwd_product_activity_rel rel
                  ON p.product_id = rel.product_id
-- 关联客服发送优惠明细（获取优惠发送和使用数据）
        LEFT JOIN dwd_customer_service_send_detail send
                  ON p.product_id = send.product_id
                      AND (rel.activity_id = send.activity_id OR rel.activity_id IS NULL) -- 关联同一活动或无活动记录
-- 关联统计周期维度（兼容低版本Hive）
        CROSS JOIN (
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
-- 过滤条件：活动关联时间或发送时间在统计周期内
WHERE (rel.add_time IS NULL OR DATE(rel.add_time) BETWEEN stats.start_date AND stats.end_date)
  AND (send.send_time IS NULL OR DATE(send.send_time) BETWEEN stats.start_date AND stats.end_date)
-- 按商品和统计周期分组
GROUP BY p.product_id,
         p.product_name,
         stats.period,
         stats.start_date,
         stats.end_date;

select *
from dws_product_discount_summary;