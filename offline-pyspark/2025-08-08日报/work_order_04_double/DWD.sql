USE rewrite_work_order_04;

-- TODO 1. dwd_sales_detail（销售明细事实表）
DROP TABLE IF EXISTS dwd_sales_detail;
CREATE TABLE IF NOT EXISTS dwd_sales_detail
(
    product_id    STRING COMMENT '商品ID',
    category_id   STRING COMMENT '品类ID',
    product_name  STRING COMMENT '商品名称',
    category_name STRING COMMENT '品类名称',
    pay_amount    DOUBLE COMMENT '支付金额',
    pay_num       INT COMMENT '支付件数',
    stat_date     STRING COMMENT '统计日期（yyyy-MM-dd）'
)
    COMMENT '销售明细事实表，清洗自ods_sales_order'
    PARTITIONED BY (dt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

-- 开启动态分区配置，支持按日期分区写入
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

-- 插入销售明细事实表数据（修复子查询关联问题）
INSERT INTO TABLE dwd_sales_detail PARTITION (dt)
SELECT
    -- 商品ID：直接取自订单表
    o.product_id,
    -- 品类ID：直接取自订单表
    o.category_id,
    -- 商品名称：保留原始商品名称
    o.product_name,
    -- 品类名称：直接取自订单表
    o.category_name,
    -- 支付金额：清洗负值
    case when o.pay_amount < 0 then 0 else o.pay_amount end as pay_amount,
    -- 支付件数：清洗不合理值
    case when o.pay_num < 1 then 1 else o.pay_num end as pay_num,
    -- 统计日期：从下单时间提取
    substr(o.order_time, 1, 10) as stat_date,
    -- 分区dt：与订单表一致
    o.dt
FROM
    ods_sales_order o
-- 关联商品属性表（修正关联条件位置）
        left join (
        select
            product_id,
            dt,  -- 保留分区字段用于关联
            concat_ws(';', collect_list(concat(property_name, ':', property_value))) as core_properties
        from ods_product_property
        group by product_id, dt  -- 按商品ID和分区分组
    ) p on o.product_id = p.product_id
        and o.dt = p.dt  -- 分区关联条件移至ON子句
-- 过滤无效订单
where
    o.order_id is not null
  and o.order_id != ''
  and o.pay_amount > 0;


select * from dwd_sales_detail;
-- TODO 2. dwd_property_detail（属性明细事实表）
DROP TABLE IF EXISTS dwd_property_detail;
CREATE TABLE IF NOT EXISTS dwd_property_detail
(
    product_id          STRING COMMENT '商品ID',
    category_id         STRING COMMENT '品类ID',
    property_name       STRING COMMENT '属性名称',
    property_value      STRING COMMENT '属性值',
    traffic_num         INT COMMENT '属性流量（该属性对应的商品访客数）',
    pay_conversion_rate DOUBLE COMMENT '属性支付转化率',
    active_product_num  INT COMMENT '属性动销商品数',
    stat_date           STRING COMMENT '统计日期'
)
    COMMENT '属性明细事实表，关联商品属性和流量数据'
    PARTITIONED BY (dt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

-- 开启动态分区配置
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

-- 插入属性明细事实表数据（修复字段引用错误）
INSERT INTO TABLE dwd_property_detail PARTITION (dt)
SELECT
    -- 商品ID：取自商品属性表
    p.product_id,
    -- 品类ID：取自商品属性表
    p.category_id,
    -- 属性名称：直接取自商品属性表
    p.property_name,
    -- 属性值：直接取自商品属性表
    p.property_value,
    -- 属性流量：该属性对应的商品总访客数（去重用户数）
    t.traffic_num,
    -- 属性支付转化率：支付用户数/访客数（保留4位小数）
    round(if(t.traffic_num > 0, t.pay_user_num / t.traffic_num, 0), 4) as pay_conversion_rate,
    -- 属性动销商品数：从agg_s子查询获取（修正字段引用来源）
    agg_s.active_product_num,
    -- 统计日期：与分区日期一致
    p.dt as stat_date,
    -- 分区dt：按日分区，与源表保持一致
    p.dt
FROM
    -- 商品属性表作为主表
    ods_product_property p
-- 关联流量统计子查询（计算每个商品的访客数和支付用户数）
        left join (
        select
            product_id,
            dt,
            count(distinct user_id) as traffic_num,  -- 去重访客数
            count(distinct case when is_pay = 1 then user_id end) as pay_user_num  -- 去重支付用户数
        from ods_traffic_log
        group by product_id, dt
    ) t on p.product_id = t.product_id and p.dt = t.dt
-- 关联销售统计子查询（标记商品是否有支付记录，用于agg_s计算）
        left join (
        select
            o.product_id,
            o.dt,
            max(case when o.pay_amount > 0 then 1 else 0 end) as is_active  -- 1=有支付，0=无
        from ods_sales_order o
        group by o.product_id, o.dt
    ) s on p.product_id = s.product_id and p.dt = s.dt
-- 聚合计算属性维度的动销商品数（核心子查询，提供active_product_num）
        left join (
        select
            p2.property_name,
            p2.property_value,
            p2.dt,
            -- 统计该属性下有支付记录的商品数（去重）
            count(distinct case when s2.is_active = 1 then p2.product_id end) as active_product_num
        from ods_product_property p2
                 left join (
            select product_id, dt, max(case when pay_amount > 0 then 1 else 0 end) as is_active
            from ods_sales_order
            group by product_id, dt
        ) s2 on p2.product_id = s2.product_id and p2.dt = s2.dt
        group by p2.property_name, p2.property_value, p2.dt
    ) agg_s on p.property_name = agg_s.property_name
        and p.property_value = agg_s.property_value
        and p.dt = agg_s.dt
-- 过滤无效属性记录
where
    p.product_id is not null
  and p.property_name is not null
  and p.property_value is not null;

select * from dwd_property_detail;
-- TODO 3. dwd_traffic_detail（流量明细事实表）
DROP TABLE IF EXISTS dwd_traffic_detail;
CREATE TABLE IF NOT EXISTS dwd_traffic_detail
(
    category_id STRING COMMENT '品类ID',
    channel     STRING COMMENT '流量渠道',
    search_word STRING COMMENT '搜索关键词',
    visitor_num INT COMMENT '渠道访客数',
    pay_num     INT COMMENT '渠道支付人数',
    stat_date   STRING COMMENT '统计日期'
)
    COMMENT '流量明细事实表，清洗自ods_traffic_log'
    PARTITIONED BY (dt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

-- 开启动态分区配置
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

-- 插入流量明细事实表数据
INSERT INTO TABLE dwd_traffic_detail PARTITION (dt)
SELECT
    -- 品类ID：取自流量日志表
    t.category_id,
    -- 流量渠道：取自流量日志表（如手淘搜索、直播带货）
    t.channel,
    -- 搜索关键词：保留原始关键词（非搜索渠道为NULL）
    t.search_word,
    -- 渠道访客数：按维度去重统计用户数
    count(distinct t.user_id) as visitor_num,
    -- 渠道支付人数：统计该渠道中完成支付的去重用户数
    count(distinct case when t.is_pay = 1 then t.user_id end) as pay_num,
    -- 统计日期：从访问时间提取（yyyy-MM-dd）
    substr(t.visitor_time, 1, 10) as stat_date,
    -- 分区dt：与源表分区保持一致（按日分区）
    t.dt
FROM
    ods_traffic_log t
-- 可关联商品表补充品类名称（按需添加）
        left join ods_product_property p
                  on t.product_id = p.product_id
                      and t.dt = p.dt
-- 按核心维度分组聚合
group by
    t.category_id,
    t.channel,
    t.search_word,
    substr(t.visitor_time, 1, 10),  -- 统计日期
    t.dt  -- 分区字段
-- 过滤无效流量记录
having
    t.category_id is not null
   and t.channel is not null;

select * from dwd_traffic_detail;
-- TODO 4. dwd_crowd_detail（人群明细事实表）
DROP TABLE IF EXISTS dwd_crowd_detail;
CREATE TABLE IF NOT EXISTS dwd_crowd_detail
(
    category_id     STRING COMMENT '品类ID',
    crowd_type      STRING COMMENT '人群类型（search/visit/pay）',
    age_range       STRING COMMENT '年龄段',
    gender          STRING COMMENT '性别',
    region          STRING COMMENT '地域',
    user_count      INT COMMENT '人群数量',
    stay_duration   DOUBLE COMMENT '平均停留时长（秒）',
    add_cart_rate   DOUBLE COMMENT '加购率',
    repurchase_rate DOUBLE COMMENT '复购率',
    stat_date       STRING COMMENT '统计日期'
)
    COMMENT '人群明细事实表，关联流量日志和用户画像'
    PARTITIONED BY (dt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

-- 开启动态分区配置
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

-- 插入人群明细事实表数据（修复子查询引用外部表问题）
INSERT INTO TABLE dwd_crowd_detail PARTITION (dt)
SELECT
    -- 品类ID：取自流量日志表
    t.category_id,
    -- 人群类型：按用户行为划分
    case
        when t.search_word is not null then 'search'
        when o.order_id is not null then 'pay'
        else 'visit'
        end as crowd_type,
    -- 年龄段：取自用户画像表
    u.age_range,
    -- 性别：取自用户画像表
    u.gender,
    -- 地域：取自用户画像表
    u.region,
    -- 人群数量：去重用户数
    count(distinct t.user_id) as user_count,
    -- 平均停留时长
    round(avg(t.stay_duration), 1) as stay_duration,
    -- 加购率
    round(
            if(count(distinct t.user_id) > 0,
               count(distinct case when t.is_pay = 1 then t.user_id end) / count(distinct t.user_id),
               0
            ), 4
    ) as add_cart_rate,
    -- 复购率：修复子查询引用问题
    round(
            if(count(distinct case when o.order_id is not null then t.user_id end) > 0,
               sum(case when oc.user_order_cnt >= 2 then 1 else 0 end)
                   / count(distinct case when o.order_id is not null then t.user_id end),
               0
            ), 4
    ) as repurchase_rate,
    -- 统计日期
    substr(t.visitor_time, 1, 10) as stat_date,
    -- 分区dt
    t.dt
FROM
    ods_traffic_log t
-- 关联用户画像表
        left join ods_user_profile u
                  on t.user_id = u.user_id
                      and t.dt = u.dt
-- 关联销售订单表
        left join ods_sales_order o
                  on t.user_id = o.user_id
                      and t.product_id = o.product_id
                      and t.dt = o.dt
-- 子查询：计算用户30天内的订单数（修正外部表引用问题）
        left join (
        select
            user_id,
            product_id,
            dt as order_dt,  -- 保留订单表的dt用于关联
            count(distinct order_id) as user_order_cnt
        from ods_sales_order
        group by user_id, product_id, dt  -- 按用户、商品、日期分组
    ) oc on t.user_id = oc.user_id
        and t.product_id = oc.product_id
        and oc.order_dt >= date_sub(t.dt, 30)  -- 日期条件移至ON子句
        and oc.order_dt <= t.dt
-- 按核心维度分组
group by
    t.category_id,
    case
        when t.search_word is not null then 'search'
        when o.order_id is not null then 'pay'
        else 'visit'
        end,
    u.age_range,
    u.gender,
    u.region,
    substr(t.visitor_time, 1, 10),
    t.dt
-- 过滤无效记录
having
    t.category_id is not null
   and u.age_range is not null;

select * from dwd_crowd_detail;