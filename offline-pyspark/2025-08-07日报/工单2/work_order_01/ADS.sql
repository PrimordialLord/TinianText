-- ADS层：面向业务场景，提供直接可用的统计分析结果
USE work_order_01_double;
-- 商品宏观监控看板指标表
CREATE TABLE IF NOT EXISTS ads_commodity_macro_monitor_board
(
    statistic_date
    DATE
    COMMENT
    '统计日期',
    time_dimension
    STRING
    COMMENT
    '时间维度',
    commodity_id
    BIGINT
    COMMENT
    '商品ID',
    commodity_name
    STRING
    COMMENT
    '商品名称',
    category_id
    BIGINT
    COMMENT
    '类目ID',
    category_name
    STRING
    COMMENT
    '类目名称',
    -- 核心指标
    visitor_count
    INT
    COMMENT
    '商品访客数',
    pay_amount
    DECIMAL
(
    10,
    2
) COMMENT '支付金额',
    pay_quantity INT COMMENT '支付件数',
    pay_conversion DECIMAL
(
    5,
    4
) COMMENT '支付转化率',
    -- 趋势指标
    visitor_count_trend STRING COMMENT '访客数趋势（升/降/平）',
    pay_amount_trend STRING COMMENT '支付金额趋势（升/降/平）',
    pay_conversion_trend STRING COMMENT '支付转化率趋势（升/降/平）',
    -- 异常指标
    is_visitor_abnormal TINYINT COMMENT '访客数是否异常（0-否，1-是）',
    is_pay_abnormal TINYINT COMMENT '支付金额是否异常（0-否，1-是）',
    -- 排名指标
    pay_amount_rank INT COMMENT '支付金额排名',
    pay_conversion_rank INT COMMENT '支付转化率排名',
    etl_time TIMESTAMP COMMENT 'ETL处理时间'
    ) COMMENT '商品宏观监控看板指标表'
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\t'
    STORED AS ORC;

-- 向商品宏观监控看板指标表插入数据
INSERT INTO TABLE ads_commodity_macro_monitor_board
SELECT
    -- 统计日期
    dws.statistic_date AS statistic_date,
    -- 时间维度（保留DWS层的日/周/月等维度）
    dws.time_dimension AS time_dimension,
    -- 商品及类目基础信息
    dws.commodity_id AS commodity_id,
    dws.commodity_name AS commodity_name,
    dws.category_id AS category_id,
    dws.category_name AS category_name,
    -- 核心指标（取自DWS层宏观监控表）
    dws.visitor_count AS visitor_count,
    dws.pay_amount AS pay_amount,
    dws.pay_quantity AS pay_quantity,
    dws.pay_conversion AS pay_conversion,
    -- 趋势指标（与上一周期对比）
    CASE
        WHEN dws.visitor_count > prev.visitor_count THEN '升'
        WHEN dws.visitor_count < prev.visitor_count THEN '降'
        ELSE '平'
        END AS visitor_count_trend,
    CASE
        WHEN dws.pay_amount > prev.pay_amount THEN '升'
        WHEN dws.pay_amount < prev.pay_amount THEN '降'
        ELSE '平'
        END AS pay_amount_trend,
    CASE
        WHEN dws.pay_conversion > prev.pay_conversion THEN '升'
        WHEN dws.pay_conversion < prev.pay_conversion THEN '降'
        ELSE '平'
        END AS pay_conversion_trend,
    -- 异常指标（偏离类目均值20%以上视为异常）
    CASE
        WHEN dws.visitor_count > avg_visitor.avg_visitor * 1.2
            OR dws.visitor_count < avg_visitor.avg_visitor * 0.8
            THEN 1 ELSE 0
        END AS is_visitor_abnormal,
    CASE
        WHEN dws.pay_amount > avg_pay.avg_pay * 1.2
            OR dws.pay_amount < avg_pay.avg_pay * 0.8
            THEN 1 ELSE 0
        END AS is_pay_abnormal,
    -- 排名指标（在类目内按时间维度排名）
    ROW_NUMBER() OVER (
        PARTITION BY dws.category_id, dws.time_dimension
        ORDER BY dws.pay_amount DESC
        ) AS pay_amount_rank,
    ROW_NUMBER() OVER (
        PARTITION BY dws.category_id, dws.time_dimension
        ORDER BY dws.pay_conversion DESC
        ) AS pay_conversion_rank,
    current_timestamp() AS etl_time
FROM dws_commodity_macro_monitor dws
-- 关联上一周期数据用于趋势分析
         LEFT JOIN dws_commodity_macro_monitor prev
                   ON dws.commodity_id = prev.commodity_id
                       AND dws.time_dimension = prev.time_dimension
                       AND CASE
                          -- 按时间维度匹配上一周期（日→前1天，周→前1周，月→前1月）
                               WHEN dws.time_dimension = '日'
                                   THEN dws.statistic_date = date_add(prev.statistic_date, 1)
                               WHEN dws.time_dimension = '周'
                                   THEN dws.statistic_date = date_add(prev.statistic_date, 7)
                               WHEN dws.time_dimension = '月'
                                   THEN dws.statistic_date = add_months(prev.statistic_date, 1)
                               WHEN dws.time_dimension = '7天'
                                   THEN dws.statistic_date = date_add(prev.statistic_date, 7)
                               WHEN dws.time_dimension = '30天'
                                   THEN dws.statistic_date = date_add(prev.statistic_date, 30)
                          END
-- 关联类目平均访客数用于异常判断
         LEFT JOIN (
    SELECT
        category_id,
        time_dimension,
        AVG(visitor_count) AS avg_visitor
    FROM dws_commodity_macro_monitor
    GROUP BY category_id, time_dimension
) avg_visitor
                   ON dws.category_id = avg_visitor.category_id
                       AND dws.time_dimension = avg_visitor.time_dimension
-- 关联类目平均支付金额用于异常判断
         LEFT JOIN (
    SELECT
        category_id,
        time_dimension,
        AVG(pay_amount) AS avg_pay
    FROM dws_commodity_macro_monitor
    GROUP BY category_id, time_dimension
) avg_pay
                   ON dws.category_id = avg_pay.category_id
                       AND dws.time_dimension = avg_pay.time_dimension;

select * from ads_commodity_macro_monitor_board;
-- 商品区间分析看板指标表
CREATE TABLE IF NOT EXISTS ads_commodity_interval_analysis_board
(
    statistic_date
    DATE
    COMMENT
    '统计日期',
    time_dimension
    STRING
    COMMENT
    '时间维度',
    category_id
    BIGINT
    COMMENT
    '类目ID（叶子类目）',
    category_name
    STRING
    COMMENT
    '类目名称',
    interval_type
    STRING
    COMMENT
    '区间类型',
    interval_range
    STRING
    COMMENT
    '区间范围',
    active_commodity_count
    INT
    COMMENT
    '动销商品数',
    active_commodity_ratio
    DECIMAL
(
    5,
    4
) COMMENT '动销率',
    total_pay_amount DECIMAL
(
    10,
    2
) COMMENT '总支付金额',
    pay_amount_ratio DECIMAL
(
    5,
    4
) COMMENT '支付金额占比',
    total_pay_quantity INT COMMENT '总支付件数',
    average_price DECIMAL
(
    10,
    2
) COMMENT '件单价',
    -- 排序字段
    active_rank INT COMMENT '按动销商品数排序',
    pay_amount_rank INT COMMENT '按支付金额排序',
    etl_time TIMESTAMP COMMENT 'ETL处理时间'
    ) COMMENT '商品区间分析看板指标表'
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\t'
    STORED AS ORC;

-- 向商品区间分析看板指标表插入数据
INSERT INTO TABLE ads_commodity_interval_analysis_board
SELECT
    -- 统计日期
    dws.statistic_date AS statistic_date,
    -- 时间维度（保留DWS层的日/周/月维度）
    dws.time_dimension AS time_dimension,
    -- 叶子类目信息
    dws.category_id AS category_id,
    dws.category_name AS category_name,
    -- 区间类型及范围
    dws.interval_type AS interval_type,
    dws.interval_range AS interval_range,
    -- 基础指标（取自DWS层）
    dws.active_commodity_count AS active_commodity_count,
    -- 动销率（当前区间动销商品数/类目总动销商品数）
    CASE
        WHEN SUM(dws.active_commodity_count) OVER (PARTITION BY dws.category_id, dws.time_dimension, dws.interval_type) = 0
            THEN 0
        ELSE dws.active_commodity_count * 1.0
            / SUM(dws.active_commodity_count) OVER (PARTITION BY dws.category_id, dws.time_dimension, dws.interval_type)
        END AS active_commodity_ratio,
    -- 支付金额及占比
    dws.total_pay_amount AS total_pay_amount,
    CASE
        WHEN SUM(dws.total_pay_amount) OVER (PARTITION BY dws.category_id, dws.time_dimension, dws.interval_type) = 0
            THEN 0
        ELSE dws.total_pay_amount * 1.0
            / SUM(dws.total_pay_amount) OVER (PARTITION BY dws.category_id, dws.time_dimension, dws.interval_type)
        END AS pay_amount_ratio,
    -- 其他基础指标
    dws.total_pay_quantity AS total_pay_quantity,
    dws.average_price AS average_price,
    -- 排序字段（在类目和区间类型内排序）
    ROW_NUMBER() OVER (
        PARTITION BY dws.category_id, dws.time_dimension, dws.interval_type
        ORDER BY dws.active_commodity_count DESC
        ) AS active_rank,
    ROW_NUMBER() OVER (
        PARTITION BY dws.category_id, dws.time_dimension, dws.interval_type
        ORDER BY dws.total_pay_amount DESC
        ) AS pay_amount_rank,
    current_timestamp() AS etl_time
FROM dws_commodity_interval_analysis dws;

select * from ads_commodity_interval_analysis_board;


select * from ads_commodity_interval_analysis_board;
select * from ads_commodity_macro_monitor_board;