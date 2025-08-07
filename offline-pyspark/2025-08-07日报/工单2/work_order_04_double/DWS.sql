USE rewrite_work_order_04;
show Tables;
-- TODO 1. dws_category_sales_summary（品类销售汇总表）
DROP TABLE IF EXISTS dws_category_sales_summary;
CREATE TABLE IF NOT EXISTS dws_category_sales_summary
(
    category_id                STRING COMMENT '品类ID',
    category_name              STRING COMMENT '品类名称',
    total_sales_amount         DOUBLE COMMENT '总销售额',
    total_sales_num            INT COMMENT '总销量',
    monthly_target_gmv         DOUBLE COMMENT '月目标GMV',
    monthly_sales_contribution DOUBLE COMMENT '月销售额占比',
    category_rank              INT COMMENT '品类排名',
    time_dimension             STRING COMMENT '时间维度（day/week/month）',
    stat_date                  STRING COMMENT '统计日期'
)
    COMMENT '品类销售汇总表，支撑ADS销售可视化'
    PARTITIONED BY (dt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

-- 开启动态分区和压缩配置
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.compress.output=true;

-- 插入品类销售汇总数据（移除目标表关联）
INSERT INTO TABLE dws_category_sales_summary PARTITION (dt)
SELECT
    s.category_id,
    s.category_name,
    SUM(s.pay_amount) AS total_sales_amount,
    SUM(s.pay_num) AS total_sales_num,
    0 AS monthly_target_gmv,  -- 用0代替目标值，或改为业务默认值
    ROUND(
            SUM(s.pay_amount) / SUM(SUM(s.pay_amount)) OVER (),
            4
    ) AS monthly_sales_contribution,
    ROW_NUMBER() OVER (ORDER BY SUM(s.pay_amount) DESC) AS category_rank,
    CASE
        WHEN LENGTH(s.stat_date) = 10 THEN 'day'
        WHEN SUBSTR(s.stat_date, 8, 2) BETWEEN '01' AND '07' THEN 'week'
        ELSE 'month'
        END AS time_dimension,
    s.stat_date,
    s.dt
FROM
    dwd_sales_detail s  -- 仅保留销售明细表
GROUP BY
    s.category_id,
    s.category_name,
    s.stat_date,
    s.dt  -- 移除与目标表相关的分组字段
HAVING
    s.category_id IS NOT NULL
   AND s.stat_date IS NOT NULL;

select * from dws_category_sales_summary;
-- TODO 2. dws_category_property_summary（品类属性汇总表）
DROP TABLE IF EXISTS dws_category_property_summary;
CREATE TABLE IF NOT EXISTS dws_category_property_summary
(
    category_id           STRING COMMENT '品类ID',
    category_name         STRING COMMENT '品类名称',
    property_name         STRING COMMENT '属性名称',
    property_value        STRING COMMENT '属性值',
    traffic_num           INT COMMENT '属性流量',
    pay_conversion_rate   DOUBLE COMMENT '支付转化率',
    active_product_num    INT COMMENT '动销商品数',
    property_sales_amount DOUBLE COMMENT '属性销售额', -- 关联销售数据
    time_dimension        STRING COMMENT '时间维度',
    stat_date             STRING COMMENT '统计日期'
)
    COMMENT '品类属性汇总表，支撑ADS属性可视化'
    PARTITIONED BY (dt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

-- 开启动态分区和压缩配置
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.compress.output=true;

-- 插入品类属性汇总数据
INSERT INTO TABLE dws_category_property_summary PARTITION (dt)
SELECT
    -- 品类ID：取自属性明细表
    p.category_id,
    -- 品类名称：从销售明细表关联获取（确保名称一致性）
    MAX(s.category_name) AS category_name,
    -- 属性名称：取自属性明细表
    p.property_name,
    -- 属性值：取自属性明细表
    p.property_value,
    -- 属性流量：属性对应的总访客数（去重汇总）
    SUM(p.traffic_num) AS traffic_num,
    -- 支付转化率：总支付转化数/总流量（保留4位小数）
    ROUND(
            SUM(p.traffic_num * p.pay_conversion_rate) / NULLIF(SUM(p.traffic_num), 0),
            4
    ) AS pay_conversion_rate,
    -- 动销商品数：属性下有销售的商品总数（去重）
    SUM(p.active_product_num) AS active_product_num,
    -- 属性销售额：关联销售明细表计算属性对应的总销售额
    SUM(s.pay_amount) AS property_sales_amount,
    -- 时间维度：根据统计日期判断（日/周/月）
    CASE
        WHEN LENGTH(p.stat_date) = 10 THEN 'day'
        WHEN SUBSTR(p.stat_date, 8, 2) BETWEEN '01' AND '07' THEN 'week'
        ELSE 'month'
        END AS time_dimension,
    -- 统计日期：取自属性明细表
    p.stat_date,
    -- 分区日期：与源表保持一致
    p.dt
FROM
    -- 主表：属性明细事实表
    dwd_property_detail p
-- 关联销售明细表，获取商品销售额和品类名称
        LEFT JOIN (
        SELECT
            product_id,
            category_id,
            category_name,
            stat_date,
            dt,
            SUM(pay_amount) AS pay_amount  -- 商品级销售额汇总
        FROM dwd_sales_detail
        GROUP BY product_id, category_id, category_name, stat_date, dt
    ) s ON p.product_id = s.product_id
        AND p.stat_date = s.stat_date
        AND p.dt = s.dt
-- 按核心维度分组聚合
GROUP BY
    p.category_id,
    p.property_name,
    p.property_value,
    p.stat_date,
    p.dt
-- 过滤无效数据
HAVING
    p.category_id IS NOT NULL
   AND p.property_name IS NOT NULL
   AND p.property_value IS NOT NULL;

select * from dws_category_property_summary;
-- TODO 3. dws_category_traffic_summary（品类流量汇总表）
DROP TABLE IF EXISTS dws_category_traffic_summary;
CREATE TABLE IF NOT EXISTS dws_category_traffic_summary
(
    category_id             STRING COMMENT '品类ID',
    category_name           STRING COMMENT '品类名称',
    channel                 STRING COMMENT '流量渠道',
    visitor_ratio           DOUBLE COMMENT '渠道访客占比',
    channel_conversion_rate DOUBLE COMMENT '渠道转化率',
    search_word             STRING COMMENT '搜索关键词',
    search_visitor_num      INT COMMENT '关键词访客数',
    search_conversion_rate  DOUBLE COMMENT '关键词转化率',
    stat_date               STRING COMMENT '统计日期'
)
    COMMENT '品类流量汇总表，支撑ADS流量可视化'
    PARTITIONED BY (dt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

-- 开启动态分区和压缩配置
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.compress.output=true;

-- 插入品类流量汇总数据
INSERT INTO TABLE dws_category_traffic_summary PARTITION (dt)
SELECT
    -- 品类ID：取自流量明细表
    t.category_id,
    -- 品类名称：从销售明细表关联获取
    MAX(s.category_name) AS category_name,
    -- 流量渠道：取自流量明细表
    t.channel,
    -- 渠道访客占比：渠道访客数/品类总访客数（保留4位小数）
    ROUND(
            SUM(t.visitor_num) / NULLIF(SUM(SUM(t.visitor_num)) OVER (PARTITION BY t.category_id), 0),
            4
    ) AS visitor_ratio,
    -- 渠道转化率：渠道支付人数/渠道访客数（保留4位小数）
    ROUND(
            SUM(t.pay_num) / NULLIF(SUM(t.visitor_num), 0),
            4
    ) AS channel_conversion_rate,
    -- 搜索关键词：取自流量明细表（非搜索渠道为NULL）
    t.search_word,
    -- 关键词访客数：同一关键词的总访客数
    SUM(t.visitor_num) AS search_visitor_num,
    -- 关键词转化率：关键词支付人数/关键词访客数（保留4位小数）
    ROUND(
            SUM(t.pay_num) / NULLIF(SUM(t.visitor_num), 0),
            4
    ) AS search_conversion_rate,
    -- 统计日期：取自流量明细表
    t.stat_date,
    -- 分区日期：与源表保持一致
    t.dt
FROM
    -- 主表：流量明细事实表
    dwd_traffic_detail t
-- 关联销售明细表获取品类名称
        LEFT JOIN (
        SELECT
            DISTINCT category_id,
                     category_name,
                     stat_date,
                     dt
        FROM dwd_sales_detail
    ) s ON t.category_id = s.category_id
        AND t.stat_date = s.stat_date
        AND t.dt = s.dt
-- 按核心维度分组聚合
GROUP BY
    t.category_id,
    t.channel,
    t.search_word,
    t.stat_date,
    t.dt
-- 过滤无效数据
HAVING
    t.category_id IS NOT NULL
   AND t.channel IS NOT NULL;

select * from dws_category_traffic_summary;
-- TODO 4. dws_category_crowd_summary（品类人群汇总表）
DROP TABLE IF EXISTS dws_category_crowd_summary;
CREATE TABLE IF NOT EXISTS dws_category_crowd_summary
(
    category_id     STRING COMMENT '品类ID',
    category_name   STRING COMMENT '品类名称',
    crowd_type      STRING COMMENT '人群类型',
    age_ratio       DOUBLE COMMENT '年龄段占比',
    gender_ratio    DOUBLE COMMENT '性别占比',
    region_ratio    DOUBLE COMMENT '地域占比',
    stay_duration   DOUBLE COMMENT '平均停留时长',
    add_cart_rate   DOUBLE COMMENT '加购率',
    repurchase_rate DOUBLE COMMENT '复购率',
    flow_type       STRING COMMENT '流转类型（如search→visit）',
    flow_ratio      DOUBLE COMMENT '流转率',
    stat_date       STRING COMMENT '统计日期'
)
    COMMENT '品类流量汇总表，支撑ADS流量可视化'
    PARTITIONED BY (dt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

-- 开启动态分区和压缩配置
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.compress.output=true;

-- 插入品类人群汇总数据
INSERT INTO TABLE dws_category_crowd_summary PARTITION (dt)
SELECT
    -- 品类ID：取自人群明细表
    c.category_id,
    -- 品类名称：从销售明细表关联获取
    MAX(s.category_name) AS category_name,
    -- 人群类型：取自人群明细表（search/visit/pay）
    c.crowd_type,
    -- 年龄段占比：该人群中某年龄段用户占比（保留4位小数）
    ROUND(
            SUM(c.user_count) / NULLIF(SUM(SUM(c.user_count)) OVER (PARTITION BY c.category_id, c.crowd_type), 0),
            4
    ) AS age_ratio,
    -- 性别占比：该人群中某性别用户占比（保留4位小数）
    ROUND(
            SUM(c.user_count) / NULLIF(SUM(SUM(c.user_count)) OVER (PARTITION BY c.category_id, c.crowd_type), 0),
            4
    ) AS gender_ratio,
    -- 地域占比：该人群中某地域用户占比（保留4位小数）
    ROUND(
            SUM(c.user_count) / NULLIF(SUM(SUM(c.user_count)) OVER (PARTITION BY c.category_id, c.crowd_type), 0),
            4
    ) AS region_ratio,
    -- 平均停留时长：该人群的平均停留时长（保留1位小数）
    ROUND(AVG(c.stay_duration), 1) AS stay_duration,
    -- 加购率：该人群的平均加购率（保留4位小数）
    ROUND(AVG(c.add_cart_rate), 4) AS add_cart_rate,
    -- 复购率：该人群的平均复购率（保留4位小数）
    ROUND(AVG(c.repurchase_rate), 4) AS repurchase_rate,
    -- 流转类型：人群流转路径（如search→visit）
    CONCAT(prev_crowd.crowd_type, '→', c.crowd_type) AS flow_type,
    -- 流转率：上一层人群流转到当前人群的比例（保留4位小数）
    ROUND(
            SUM(c.user_count) / NULLIF(SUM(prev_crowd.user_count), 0),
            4
    ) AS flow_ratio,
    -- 统计日期：取自人群明细表
    c.stat_date,
    -- 分区日期：与源表保持一致
    c.dt
FROM
    -- 主表：人群明细事实表（当前人群）
    dwd_crowd_detail c
-- 关联上一层人群数据（用于计算流转率）
        LEFT JOIN (
        SELECT
            category_id,
            crowd_type,
            stat_date,
            dt,
            SUM(user_count) AS user_count
        FROM dwd_crowd_detail
        GROUP BY category_id, crowd_type, stat_date, dt
    ) prev_crowd ON c.category_id = prev_crowd.category_id
        AND c.stat_date = prev_crowd.stat_date
        AND c.dt = prev_crowd.dt
        -- 定义流转关系：search→visit→pay
        AND (
                        (c.crowd_type = 'visit' AND prev_crowd.crowd_type = 'search') OR
                        (c.crowd_type = 'pay' AND prev_crowd.crowd_type = 'visit')
                        )
-- 关联销售明细表获取品类名称
        LEFT JOIN (
        SELECT
            DISTINCT category_id,
                     category_name,
                     stat_date,
                     dt
        FROM dwd_sales_detail
    ) s ON c.category_id = s.category_id
        AND c.stat_date = s.stat_date
        AND c.dt = s.dt
-- 按核心维度分组聚合
GROUP BY
    c.category_id,
    c.crowd_type,
    c.age_range,
    c.gender,
    c.region,
    c.stat_date,
    c.dt,
    prev_crowd.crowd_type
-- 过滤无效数据
HAVING
    c.category_id IS NOT NULL
   AND c.crowd_type IS NOT NULL
   AND c.stat_date IS NOT NULL;

select * from dws_category_crowd_summary;

