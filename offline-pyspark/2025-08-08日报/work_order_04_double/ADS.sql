USE rewrite_work_order_04;

-- TODO 1. 品类销售可视化表（ads_category_sales_visual）
DROP TABLE IF EXISTS ads_category_sales_visual;
CREATE TABLE IF NOT EXISTS ads_category_sales_visual
(
    category_id                STRING COMMENT '品类ID',
    category_name              STRING COMMENT '品类名称',
    stat_date                  STRING COMMENT '统计日期（yyyy-MM-dd）',
    time_dimension             STRING COMMENT '时间维度（day/week/month）',
    total_sales_amount         DOUBLE COMMENT '总销售额',
    total_sales_num            INT COMMENT '总销量',
    monthly_target_progress    DOUBLE COMMENT '月支付金额进度（当前销售额/目标GMV）',
    monthly_sales_contribution DOUBLE COMMENT '月支付金额贡献（品类销售额占全店比例）',
    category_rank              INT COMMENT '品类在同级类目中的上月支付金额排名',
    top_sales_products         ARRAY<STRUCT<product_id :STRING, product_name :STRING, sales_amount
                                            :DOUBLE>> COMMENT '品类内TOP10热销商品'
)
    COMMENT '品类销售可视化表，支撑销售分析相关可视化'
    PARTITIONED BY (dt STRING)
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\t'
        COLLECTION ITEMS TERMINATED BY ','
        MAP KEYS TERMINATED BY ':';

-- 强制开启动态分区配置o
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.compress.output=true;

-- 插入品类销售可视化表数据
INSERT INTO TABLE ads_category_sales_visual PARTITION (dt)
SELECT
    s.category_id,
    s.category_name,
    s.stat_date,
    s.time_dimension,
    s.total_sales_amount,
    s.total_sales_num,
    ROUND(s.total_sales_amount / NULLIF(s.monthly_target_gmv, 0), 4) AS monthly_target_progress,
    s.monthly_sales_contribution,
    s.category_rank,
    -- 核心修正：通过子查询提前定义STRUCT字段名，确保与目标表一致
    COLLECT_LIST(top_products) AS top_sales_products,
    s.dt  -- 动态分区字段
FROM (
         -- 主表：DWS层销售汇总
         SELECT
             category_id,
             category_name,
             stat_date,
             time_dimension,
             total_sales_amount,
             total_sales_num,
             monthly_target_gmv,
             monthly_sales_contribution,
             category_rank,
             dt
         FROM dws_category_sales_summary
     ) s
-- 关联DWD层销售明细，筛选并构造结构化TOP10商品
         LEFT JOIN (
    -- 子查询1：为商品字段定义明确的STRUCT结构
    SELECT
        p.category_id,
        p.stat_date,
        p.dt,
        -- 显式定义STRUCT字段名，与目标表完全一致
        NAMED_STRUCT(
                'product_id', p.product_id,
                'product_name', p.product_name,
                'sales_amount', p.pay_amount
        ) AS top_products,
        p.rn
    FROM (
             -- 子查询2：获取每个品类的TOP10商品
             SELECT
                 p_inner.category_id,
                 p_inner.stat_date,
                 p_inner.dt,
                 p_inner.product_id,
                 p_inner.product_name,
                 p_inner.pay_amount,
                 ROW_NUMBER() OVER (
                     PARTITION BY p_inner.category_id, p_inner.stat_date, p_inner.dt
                     ORDER BY p_inner.pay_amount DESC
                     ) AS rn
             FROM (
                      -- 聚合商品级销售额
                      SELECT
                          product_id,
                          product_name,
                          category_id,
                          stat_date,
                          dt,
                          SUM(pay_amount) AS pay_amount
                      FROM dwd_sales_detail
                      GROUP BY product_id, product_name, category_id, stat_date, dt
                  ) p_inner
         ) p
    WHERE p.rn <= 10  -- 仅保留TOP10商品
) p ON s.category_id = p.category_id
    AND s.stat_date = p.stat_date
    AND s.dt = p.dt
-- 按核心维度分组聚合
GROUP BY
    s.category_id,
    s.category_name,
    s.stat_date,
    s.time_dimension,
    s.total_sales_amount,
    s.total_sales_num,
    s.monthly_target_gmv,
    s.monthly_sales_contribution,
    s.category_rank,
    s.dt
HAVING
    s.category_id IS NOT NULL
   AND s.stat_date IS NOT NULL;

select * from ads_category_sales_visual;
-- TODO 2. 品类属性可视化表（ads_category_property_visual）
DROP TABLE IF EXISTS ads_category_property_visual;
CREATE TABLE IF NOT EXISTS ads_category_property_visual
(
    category_id         STRING COMMENT '品类ID',
    category_name       STRING COMMENT '品类名称',
    stat_date           STRING COMMENT '统计日期',
    time_dimension      STRING COMMENT '时间维度（day/week/month）',
    property_name       STRING COMMENT '属性名称（如颜色、材质）',
    property_value      STRING COMMENT '属性值（如红色、纯棉）',
    traffic_num         INT COMMENT '属性流量',
    pay_conversion_rate DOUBLE COMMENT '支付转化率',
    active_product_num  INT COMMENT '动销商品数',
    traffic_ratio       DOUBLE COMMENT '属性流量占比',
    sales_ratio         DOUBLE COMMENT '属性销售额占比'
)
    COMMENT '品类属性可视化表，支撑属性分析相关可视化'
    PARTITIONED BY (dt STRING)
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\t';

-- 开启动态分区配置
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.compress.output=true;

-- 插入品类属性可视化表数据
INSERT INTO TABLE ads_category_property_visual PARTITION (dt)
SELECT
    -- 品类ID：取自DWS层属性汇总表
    p.category_id,
    -- 品类名称：关联销售表获取，确保一致性
    MAX(s.category_name) AS category_name,
    -- 统计日期：取自DWS层属性汇总表
    p.stat_date,
    -- 时间维度：取自DWS层属性汇总表（day/week/month）
    p.time_dimension,
    -- 属性名称：取自DWS层属性汇总表
    p.property_name,
    -- 属性值：取自DWS层属性汇总表
    p.property_value,
    -- 属性流量：取自DWS层属性汇总表
    p.traffic_num,
    -- 支付转化率：取自DWS层属性汇总表
    p.pay_conversion_rate,
    -- 动销商品数：取自DWS层属性汇总表
    p.active_product_num,
    -- 属性流量占比：当前属性流量/品类总流量（保留4位小数）
    ROUND(
            p.traffic_num / NULLIF(SUM(p.traffic_num) OVER (PARTITION BY p.category_id, p.stat_date), 0),
            4
    ) AS traffic_ratio,
    -- 属性销售额占比：当前属性销售额/品类总销售额（保留4位小数）
    ROUND(
            p.property_sales_amount / NULLIF(SUM(p.property_sales_amount) OVER (PARTITION BY p.category_id, p.stat_date), 0),
            4
    ) AS sales_ratio,
    -- 分区日期：与源表保持一致
    p.dt
FROM
    -- 主表：DWS层品类属性汇总表
    dws_category_property_summary p
-- 关联DWD层销售表获取品类名称
        LEFT JOIN (
        SELECT
            DISTINCT category_id,
                     category_name,
                     stat_date,
                     dt
        FROM dwd_sales_detail
    ) s ON p.category_id = s.category_id
        AND p.stat_date = s.stat_date
        AND p.dt = s.dt
-- 按核心维度分组聚合
GROUP BY
    p.category_id,
    p.property_name,
    p.property_value,
    p.stat_date,
    p.time_dimension,
    p.traffic_num,
    p.pay_conversion_rate,
    p.active_product_num,
    p.property_sales_amount,
    p.dt
-- 过滤无效数据
HAVING
    p.category_id IS NOT NULL
   AND p.property_name IS NOT NULL
   AND p.property_value IS NOT NULL;

select * from ads_category_property_visual;
-- TODO 3. 品类流量可视化表（ads_category_traffic_visual）
DROP TABLE IF EXISTS ads_category_traffic_visual;
CREATE TABLE IF NOT EXISTS ads_category_traffic_visual
(
    category_id             STRING COMMENT '品类ID',
    category_name           STRING COMMENT '品类名称',
    stat_date               STRING COMMENT '统计日期',
    channel                 STRING COMMENT '流量渠道（如手淘搜索、直通车）',
    visitor_ratio           DOUBLE COMMENT '渠道访客占比',
    channel_conversion_rate DOUBLE COMMENT '渠道转化率',
    top_search_words        ARRAY<STRUCT<word :STRING, rank :INT, visitor_num :INT, conversion_rate
                                         :DOUBLE>> COMMENT 'TOP10热搜词'
)
    COMMENT '品类流量可视化表，支撑流量分析相关可视化'
    PARTITIONED BY (dt STRING)
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\t'
        COLLECTION ITEMS TERMINATED BY ','
        MAP KEYS TERMINATED BY ':';

-- 开启动态分区配置
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.compress.output=true;

-- 插入品类流量可视化表数据
INSERT INTO TABLE ads_category_traffic_visual PARTITION (dt)
SELECT
    t.category_id,
    MAX(s.category_name) AS category_name,
    t.stat_date,
    t.channel,
    t.visitor_ratio,
    t.channel_conversion_rate,
    -- 核心修正：将visitor_num强制转换为INT，匹配目标表类型
    COLLECT_LIST(
            NAMED_STRUCT(
                    'word', sw.search_word,
                    'rank', sw.rn,
                    'visitor_num', CAST(sw.visitor_num AS INT),  -- 转换为INT类型
                    'conversion_rate', sw.conversion_rate
            )
    ) AS top_search_words,
    t.dt
FROM
    dws_category_traffic_summary t
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
        LEFT JOIN (
        -- 子查询1：计算关键词排名和转化率（确保visitor_num为INT）
        SELECT
            ts.category_id,
            ts.stat_date,
            ts.dt,
            ts.channel,
            ts.search_word,
            CAST(ts.visitor_num AS INT) AS visitor_num,  -- 提前转换为INT
            ROUND(ts.pay_num / NULLIF(ts.visitor_num, 0), 4) AS conversion_rate,
            ROW_NUMBER() OVER (
                PARTITION BY ts.category_id, ts.stat_date, ts.dt, ts.channel
                ORDER BY ts.visitor_num DESC
                ) AS rn
        FROM (
                 -- 子查询2：聚合关键词数据时转换为INT
                 SELECT
                     category_id,
                     stat_date,
                     dt,
                     channel,
                     search_word,
                     CAST(SUM(visitor_num) AS INT) AS visitor_num,  -- 聚合后转换
                     SUM(pay_num) AS pay_num
                 FROM dwd_traffic_detail
                 WHERE search_word IS NOT NULL
                 GROUP BY category_id, stat_date, dt, channel, search_word
             ) ts
    ) sw ON t.category_id = sw.category_id
        AND t.stat_date = sw.stat_date
        AND t.dt = sw.dt
        AND t.channel = sw.channel
        AND sw.rn <= 10
GROUP BY
    t.category_id,
    t.stat_date,
    t.channel,
    t.visitor_ratio,
    t.channel_conversion_rate,
    t.dt
HAVING
    t.category_id IS NOT NULL
   AND t.channel IS NOT NULL
   AND t.stat_date IS NOT NULL;

select * from ads_category_traffic_visual;
-- TODO 4. 品类客群可视化表（ads_category_crowd_visual）
DROP TABLE IF EXISTS ads_category_crowd_visual;
CREATE TABLE IF NOT EXISTS ads_category_crowd_visual
(
    category_id         STRING COMMENT '品类ID',
    category_name       STRING COMMENT '品类名称',
    stat_date           STRING COMMENT '统计日期',
    crowd_type          STRING COMMENT '人群类型（search/visit/pay）',
    age_distribution    MAP<STRING, DOUBLE> COMMENT '年龄分布（如"20-25岁":0.3）',
    gender_distribution MAP<STRING, DOUBLE> COMMENT '性别分布（如"男":0.4）',
    region_distribution MAP<STRING, DOUBLE> COMMENT '地域分布',
    stay_duration       DOUBLE COMMENT '平均停留时长（秒）',
    add_cart_rate       DOUBLE COMMENT '加购率',
    repurchase_rate     DOUBLE COMMENT '复购率',
    crowd_flow_ratio    MAP<STRING, DOUBLE> COMMENT '人群流转率（如"search→visit":0.5）'
)
    COMMENT '品类客群可视化表，支撑客群洞察相关可视化'
    PARTITIONED BY (dt STRING)
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\t'
        COLLECTION ITEMS TERMINATED BY ','
        MAP KEYS TERMINATED BY ':';

-- 开启动态分区配置
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.compress.output=true;

-- 插入品类客群可视化表数据
INSERT INTO TABLE ads_category_crowd_visual PARTITION (dt)
SELECT
    main.category_id,
    MAX(s.category_name) AS category_name,
    main.stat_date,
    main.crowd_type,
    -- 年龄分布：用map函数手动组装，确保值为DOUBLE
    map(
            collect_list(age.age_range)[0], collect_list(age.age_ratio)[0],
            collect_list(age.age_range)[1], collect_list(age.age_ratio)[1],
            collect_list(age.age_range)[2], collect_list(age.age_ratio)[2]
        -- 可根据实际维度数量扩展，最多支持100个键值对
    ) AS age_distribution,
    -- 性别分布：同理手动组装
    map(
            collect_list(gender.gender)[0], collect_list(gender.gender_ratio)[0],
            collect_list(gender.gender)[1], collect_list(gender.gender_ratio)[1]
    ) AS gender_distribution,
    -- 地域分布：同理手动组装
    map(
            collect_list(region.region)[0], collect_list(region.region_ratio)[0],
            collect_list(region.region)[1], collect_list(region.region_ratio)[1],
            collect_list(region.region)[2], collect_list(region.region_ratio)[2],
            collect_list(region.region)[3], collect_list(region.region_ratio)[3]
    ) AS region_distribution,
    AVG(main.stay_duration) AS stay_duration,
    AVG(main.add_cart_rate) AS add_cart_rate,
    AVG(main.repurchase_rate) AS repurchase_rate,
    -- 人群流转率：手动组装MAP
    map(
            collect_list(main.flow_type)[0], collect_list(main.flow_ratio)[0],
            collect_list(main.flow_type)[1], collect_list(main.flow_ratio)[1]
    ) AS crowd_flow_ratio,
    main.dt
FROM
    dws_category_crowd_summary main
-- 关联预处理的年龄数据（确保age_ratio为DOUBLE）
        LEFT JOIN (
        SELECT
            d.category_id,
            d.crowd_type,
            d.stat_date,
            d.dt,
            d.age_range,
            CAST(
                    ROUND(
                            SUM(d.user_count) / NULLIF(SUM(SUM(d.user_count)) OVER (PARTITION BY d.category_id, d.crowd_type, d.stat_date), 0),
                            4
                    ) AS DOUBLE
            ) AS age_ratio
        FROM dwd_crowd_detail d
        GROUP BY d.category_id, d.crowd_type, d.stat_date, d.dt, d.age_range
    ) age ON main.category_id = age.category_id
        AND main.crowd_type = age.crowd_type
        AND main.stat_date = age.stat_date
        AND main.dt = age.dt
-- 关联预处理的性别数据
        LEFT JOIN (
        SELECT
            d.category_id,
            d.crowd_type,
            d.stat_date,
            d.dt,
            d.gender,
            CAST(ROUND(SUM(d.user_count) / NULLIF(SUM(SUM(d.user_count)) OVER (PARTITION BY d.category_id, d.crowd_type, d.stat_date), 0),4) AS DOUBLE) AS gender_ratio
        FROM dwd_crowd_detail d
        GROUP BY d.category_id, d.crowd_type, d.stat_date, d.dt, d.gender
    ) gender ON main.category_id = gender.category_id
        AND main.crowd_type = gender.crowd_type
        AND main.stat_date = gender.stat_date
        AND main.dt = gender.dt
-- 关联预处理的地域数据
        LEFT JOIN (
        SELECT
            d.category_id,
            d.crowd_type,
            d.stat_date,
            d.dt,
            d.region,
            CAST(ROUND(SUM(d.user_count) / NULLIF(SUM(SUM(d.user_count)) OVER (PARTITION BY d.category_id, d.crowd_type, d.stat_date), 0),4) AS DOUBLE) AS region_ratio
        FROM dwd_crowd_detail d
        GROUP BY d.category_id, d.crowd_type, d.stat_date, d.dt, d.region
    ) region ON main.category_id = region.category_id
        AND main.crowd_type = region.crowd_type
        AND main.stat_date = region.stat_date
        AND main.dt = region.dt
-- 关联销售表
        LEFT JOIN (
        SELECT DISTINCT category_id, category_name, stat_date, dt
        FROM dwd_sales_detail
    ) s ON main.category_id = s.category_id
        AND main.stat_date = s.stat_date
        AND main.dt = s.dt
-- 分组聚合
GROUP BY
    main.category_id,
    main.stat_date,
    main.crowd_type,
    main.dt
HAVING
    main.category_id IS NOT NULL
   AND main.crowd_type IS NOT NULL
   AND main.stat_date IS NOT NULL;

select * from ads_category_crowd_visual;