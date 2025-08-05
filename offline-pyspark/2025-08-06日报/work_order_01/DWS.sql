-- todo DWS层：对DWD层数据按维度汇总，形成汇总指标
USE work_order_01_double;
-- todo 商品宏观监控汇总表
CREATE TABLE IF NOT EXISTS dws_commodity_macro_monitor
(
    statistic_date
    DATE
    COMMENT
    '统计日期',
    time_dimension
    STRING
    COMMENT
    '时间维度（日、周、月、7天、30天、自定义）',
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
    visitor_count
    INT
    COMMENT
    '商品访客数',
    browse_count
    INT
    COMMENT
    '商品浏览量',
    avg_stay_duration
    DECIMAL
(
    10,
    2
) COMMENT '商品平均停留时长（秒）',
    bounce_rate DECIMAL
(
    5,
    4
) COMMENT '商品详情页跳出率',
    collect_user_count INT COMMENT '商品收藏人数',
    add_cart_quantity INT COMMENT '商品加购件数',
    add_cart_user_count INT COMMENT '商品加购人数',
    visit_collect_conversion DECIMAL
(
    5,
    4
) COMMENT '访问收藏转化率',
    visit_add_cart_conversion DECIMAL
(
    5,
    4
) COMMENT '访问加购转化率',
    order_user_count INT COMMENT '下单买家数',
    order_quantity INT COMMENT '下单件数',
    order_amount DECIMAL
(
    10,
    2
) COMMENT '下单金额',
    order_conversion DECIMAL
(
    5,
    4
) COMMENT '下单转化率',
    pay_user_count INT COMMENT '支付买家数',
    pay_quantity INT COMMENT '支付件数',
    pay_amount DECIMAL
(
    10,
    2
) COMMENT '支付金额',
    pay_conversion DECIMAL
(
    5,
    4
) COMMENT '支付转化率',
    new_pay_user_count INT COMMENT '支付新买家数',
    old_pay_user_count INT COMMENT '支付老买家数',
    refund_amount DECIMAL
(
    10,
    2
) COMMENT '成功退款退货金额',
    juhuasuan_pay_amount DECIMAL
(
    10,
    2
) COMMENT '聚划算支付金额',
    avg_visitor_value DECIMAL
(
    10,
    2
) COMMENT '访客平均价值',
    competitiveness_score INT COMMENT '竞争力评分',
    micro_detail_visitor_count INT COMMENT '商品微详情访客数',
    etl_time TIMESTAMP COMMENT 'ETL处理时间'
    ) COMMENT '商品宏观监控汇总表'
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\t'
    STORED AS ORC;

-- 生成商品宏观监控汇总表数据（以日维度为例，其他维度可通过调整时间参数实现）
INSERT INTO TABLE dws_commodity_macro_monitor
SELECT
    -- 统计日期（取访问时间日期）
    v.visit_date AS statistic_date,
    '日' AS time_dimension,  -- 时间维度：日（可扩展为周/月等）
    -- 商品及类目信息
    v.commodity_id AS commodity_id,
    v.commodity_name AS commodity_name,
    v.category_id AS category_id,
    v.category_name AS category_name,
    -- 访问指标
    COUNT(DISTINCT v.visitor_id) AS visitor_count,  -- 商品访客数（去重）
    COUNT(v.id) AS browse_count,  -- 商品浏览量
    AVG(v.stay_duration) AS avg_stay_duration,  -- 平均停留时长
    -- 跳出率：(无点击行为人数/总访客数)
    SUM(CASE WHEN v.is_click = 0 THEN 1 ELSE 0 END) / COUNT(DISTINCT v.visitor_id) AS bounce_rate,
    -- 收藏加购指标
    COUNT(DISTINCT CASE WHEN c.behavior_type = 1 THEN c.user_id END) AS collect_user_count,  -- 收藏人数
    SUM(CASE WHEN c.behavior_type = 2 THEN c.add_cart_quantity ELSE 0 END) AS add_cart_quantity,  -- 加购件数
    COUNT(DISTINCT CASE WHEN c.behavior_type = 2 THEN c.user_id END) AS add_cart_user_count,  -- 加购人数
    -- 转化率指标
    (COUNT(DISTINCT CASE WHEN c.behavior_type = 1 THEN c.user_id END) / COUNT(DISTINCT v.visitor_id)) AS visit_collect_conversion,  -- 访问收藏转化率
    (COUNT(DISTINCT CASE WHEN c.behavior_type = 2 THEN c.user_id END) / COUNT(DISTINCT v.visitor_id)) AS visit_add_cart_conversion,  -- 访问加购转化率
    -- 交易指标（下单）
    COUNT(DISTINCT t.order_id) AS order_user_count,  -- 下单买家数
    SUM(t.quantity) AS order_quantity,  -- 下单件数
    SUM(t.amount) AS order_amount,  -- 下单金额
    (COUNT(DISTINCT t.order_id) / COUNT(DISTINCT v.visitor_id)) AS order_conversion,  -- 下单转化率
    -- 交易指标（支付）
    COUNT(DISTINCT CASE WHEN t.order_status = 2 THEN t.order_id END) AS pay_user_count,  -- 支付买家数
    SUM(CASE WHEN t.order_status = 2 THEN t.quantity ELSE 0 END) AS pay_quantity,  -- 支付件数
    SUM(CASE WHEN t.order_status = 2 THEN t.amount ELSE 0 END) AS pay_amount,  -- 支付金额
    (COUNT(DISTINCT CASE WHEN t.order_status = 2 THEN t.order_id END) / COUNT(DISTINCT v.visitor_id)) AS pay_conversion,  -- 支付转化率
    -- 新老买家指标
    COUNT(DISTINCT CASE WHEN t.is_new_user = 1 AND t.order_status = 2 THEN t.user_id END) AS new_pay_user_count,  -- 支付新买家数
    COUNT(DISTINCT CASE WHEN t.is_new_user = 0 AND t.order_status = 2 THEN t.user_id END) AS old_pay_user_count,  -- 支付老买家数
    -- 其他指标
    SUM(t.refund_amount) AS refund_amount,  -- 成功退款金额
    SUM(CASE WHEN t.is_juhuasuan = 1 THEN t.amount ELSE 0 END) AS juhuasuan_pay_amount,  -- 聚划算支付金额
    (SUM(CASE WHEN t.order_status = 2 THEN t.amount ELSE 0 END) / COUNT(DISTINCT v.visitor_id)) AS avg_visitor_value,  -- 访客平均价值
    -- 竞争力评分（模拟值，实际可基于多指标计算）
    FLOOR(rand() * 100) AS competitiveness_score,
    -- 微详情访客数
    COUNT(DISTINCT CASE WHEN v.is_micro_detail = 1 THEN v.visitor_id END) AS micro_detail_visitor_count,
    current_timestamp() AS etl_time  -- ETL处理时间
-- 关联访问、收藏加购、交易表，按商品和日期聚合
FROM dwd_commodity_visit_detail v
-- 左关联收藏加购表（同商品、同日）
         LEFT JOIN dwd_commodity_collect_cart_detail c
                   ON v.commodity_id = c.commodity_id
                       AND v.visit_date = c.behavior_date
-- 左关联交易表（同商品、同日）
         LEFT JOIN dwd_commodity_transaction_detail t
                   ON v.commodity_id = t.commodity_id
                       AND v.visit_date = t.order_date
GROUP BY v.visit_date, v.commodity_id, v.commodity_name, v.category_id, v.category_name;

select * from dws_commodity_macro_monitor;

-- todo 商品区间分析汇总表
CREATE TABLE IF NOT EXISTS dws_commodity_interval_analysis
(
    statistic_date
    DATE
    COMMENT
    '统计日期',
    time_dimension
    STRING
    COMMENT
    '时间维度（日、周、月）',
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
    '区间类型（价格带、支付件数、支付金额）',
    interval_range
    STRING
    COMMENT
    '区间范围（如0~50、51~100等）',
    active_commodity_count
    INT
    COMMENT
    '动销商品数',
    total_visitor_count
    INT
    COMMENT
    '总访客数',
    total_pay_amount
    DECIMAL
(
    10,
    2
) COMMENT '总支付金额',
    total_pay_quantity INT COMMENT '总支付件数',
    average_price DECIMAL
(
    10,
    2
) COMMENT '件单价',
    pay_conversion DECIMAL
(
    5,
    4
) COMMENT '支付转化率',
    etl_time TIMESTAMP COMMENT 'ETL处理时间'
    ) COMMENT '商品区间分析汇总表'
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\t'
    STORED AS ORC;


