-- ADS层：面向业务场景，提供直接可用的统计分析结果

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
