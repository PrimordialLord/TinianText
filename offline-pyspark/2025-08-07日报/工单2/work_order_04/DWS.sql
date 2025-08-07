USE work_order_04_double;

-- todo 1. dws_category_sales_summary（品类销售汇总表）
CREATE TABLE dws_category_sales_summary (
                                            category_id STRING COMMENT '品类ID',
                                            total_sales_amount DOUBLE COMMENT '总销售额',
                                            total_sales_num INT COMMENT '总销量',
                                            sales_trend ARRAY<DOUBLE> COMMENT '销售额趋势（近30天）',
                                            monthly_sales_contribution DOUBLE COMMENT '月销售额占比',
                                            monthly_rank INT COMMENT '月销售排名',
                                            stat_date STRING COMMENT '统计日期'
)
    PARTITIONED BY (dt STRING COMMENT '分区日期')
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS ORC;

-- 关闭统计信息自动收集，避免执行报错
SET hive.stats.autogather=false;

-- 从DWD层聚合数据至DWS层品类销售汇总表（分区dt='2025-01-25'）
INSERT INTO TABLE dws_category_sales_summary PARTITION (dt='2025-01-25')
SELECT
    category_id,
    total_sales_amount,
    total_sales_num,
    -- 销售额趋势（近30天）
    COLLECT_LIST(daily_sales_amount) OVER (
        PARTITION BY category_id
        ORDER BY stat_date
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS sales_trend,
    monthly_sales_contribution,
    monthly_rank,
    stat_date
FROM (
         -- 先聚合基础指标，避免窗口函数嵌套报错
         SELECT
             dwd.category_id,
             dwd.stat_date,
             dwd.daily_sales_amount,
             -- 总销售额
             SUM(dwd.daily_sales_amount) OVER (PARTITION BY dwd.category_id) AS total_sales_amount,
             -- 总销量
             SUM(dwd.daily_sales_num) OVER (PARTITION BY dwd.category_id) AS total_sales_num,
             -- 月销售额占比
             ROUND(
                     SUM(dwd.daily_sales_amount) OVER (PARTITION BY dwd.category_id)
                         / SUM(dwd.daily_sales_amount) OVER (),
                     4
             ) AS monthly_sales_contribution,
             -- 月销售排名
             RANK() OVER (ORDER BY SUM(dwd.daily_sales_amount) OVER (PARTITION BY dwd.category_id) DESC) AS monthly_rank
         FROM dwd_category_sales_detail dwd
         WHERE dwd.dt BETWEEN DATE_SUB('2025-01-25', 29) AND '2025-01-25'
           AND dwd.stat_date = '2025-01-25'
     ) t
GROUP BY category_id, stat_date, daily_sales_amount, total_sales_amount, total_sales_num, monthly_sales_contribution, monthly_rank;

select * from dws_category_sales_summary;
-- todo 2. dws_category_property_summary（品类属性汇总表）
CREATE TABLE dws_category_property_summary (
                                               category_id STRING COMMENT '品类ID',
                                               property_name STRING COMMENT '属性名称',
                                               total_traffic INT COMMENT '属性总流量',
                                               avg_conversion_rate DOUBLE COMMENT '平均转化率',
                                               effective_property STRING COMMENT '有效属性（流量top3）',
                                               stat_date STRING COMMENT '统计日期'
)
    PARTITIONED BY (dt STRING COMMENT '分区日期')
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS ORC;

-- 关闭统计信息自动收集，避免执行报错
SET hive.stats.autogather=false;

-- 从DWD层聚合数据至DWS层品类属性汇总表（分区dt='2025-01-25'）
INSERT INTO TABLE dws_category_property_summary PARTITION (dt='2025-01-25')
SELECT
    category_id,
    property_name,
    total_traffic,
    avg_conversion_rate,
    -- 提取流量top3的有效属性（使用substr和split结合的方式替代数组切片）
    concat_ws(',',
              split(
                      concat_ws(',', COLLECT_LIST(property_value) OVER (
                          PARTITION BY category_id, property_name
                          ORDER BY total_traffic DESC
                          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                          )),
                      ','
              )[0],
              split(
                      concat_ws(',', COLLECT_LIST(property_value) OVER (
                          PARTITION BY category_id, property_name
                          ORDER BY total_traffic DESC
                          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                          )),
                      ','
              )[1],
              split(
                      concat_ws(',', COLLECT_LIST(property_value) OVER (
                          PARTITION BY category_id, property_name
                          ORDER BY total_traffic DESC
                          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                          )),
                      ','
              )[2]
    ) AS effective_property,
    stat_date
FROM (
         -- 先聚合基础指标
         SELECT
             dwd.category_id,
             dwd.property_name,
             dwd.property_value,
             dwd.stat_date,
             -- 属性总流量（汇总DWD层数据）
             SUM(dwd.traffic_num) OVER (PARTITION BY dwd.category_id, dwd.property_name) AS total_traffic,
             -- 平均转化率
             AVG(dwd.conversion_rate) OVER (PARTITION BY dwd.category_id, dwd.property_name) AS avg_conversion_rate
         FROM dwd_category_property_detail dwd
         WHERE dwd.dt = '2025-01-25'
           AND dwd.stat_date = '2025-01-25'
     ) t
GROUP BY category_id, property_name, property_value, stat_date, total_traffic, avg_conversion_rate;

select * from dws_category_property_summary;
-- todo 3. dws_category_traffic_summary（品类流量汇总表）
CREATE TABLE dws_category_traffic_summary (
                                              category_id STRING COMMENT '品类ID',
                                              channel_distribution MAP<STRING, DOUBLE> COMMENT '渠道占比',
                                              top_search_words ARRAY<STRING> COMMENT 'top3热搜词',
                                              channel_effectiveness MAP<STRING, DOUBLE> COMMENT '渠道转化率',
                                              stat_date STRING COMMENT '统计日期'
)
    PARTITIONED BY (dt STRING COMMENT '分区日期')
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS ORC;

-- 关闭统计信息自动收集，避免执行报错
SET hive.stats.autogather=false;

-- 从DWD层聚合数据至DWS层品类流量汇总表（分区dt='2025-01-25'）
INSERT INTO TABLE dws_category_traffic_summary PARTITION (dt='2025-01-25')
SELECT
    category_id,
    -- 渠道占比（手动构建map<string,double>）
    map(
            '手淘搜索', SUM(CASE WHEN channel = '手淘搜索' THEN visitor_ratio ELSE 0 END),
            '直通车', SUM(CASE WHEN channel = '直通车' THEN visitor_ratio ELSE 0 END),
            '短视频', SUM(CASE WHEN channel = '短视频' THEN visitor_ratio ELSE 0 END),
            '直播', SUM(CASE WHEN channel = '直播' THEN visitor_ratio ELSE 0 END),
            '推荐位', SUM(CASE WHEN channel = '推荐位' THEN visitor_ratio ELSE 0 END),
            'other', SUM(CASE WHEN channel = 'other' THEN visitor_ratio ELSE 0 END)
    ) AS channel_distribution,
    -- top3热搜词
    array(
            MAX(CASE WHEN search_rank = 1 THEN search_word END),
            MAX(CASE WHEN search_rank = 2 THEN search_word END),
            MAX(CASE WHEN search_rank = 3 THEN search_word END)
    ) AS top_search_words,
    -- 渠道转化率（使用随机值模拟，实际应基于业务数据）
    map(
            '手淘搜索', ROUND(RAND() * 0.3 + 0.05, 4),
            '直通车', ROUND(RAND() * 0.3 + 0.05, 4),
            '短视频', ROUND(RAND() * 0.3 + 0.05, 4),
            '直播', ROUND(RAND() * 0.3 + 0.05, 4),
            '推荐位', ROUND(RAND() * 0.3 + 0.05, 4),
            'other', ROUND(RAND() * 0.3 + 0.05, 4)
    ) AS channel_effectiveness,
    stat_date
FROM (
         -- 计算渠道访客占比和热搜词排名
         SELECT
             dwd.category_id,
             dwd.channel,
             dwd.search_word,
             dwd.stat_date,
             -- 渠道访客数占比
             ROUND(dwd.visitor_num / SUM(dwd.visitor_num) OVER (PARTITION BY dwd.category_id), 4) AS visitor_ratio,
             -- 热搜词排名
             ROW_NUMBER() OVER (PARTITION BY dwd.category_id ORDER BY dwd.search_visitor_num DESC) AS search_rank
         FROM dwd_category_traffic_detail dwd
         WHERE dwd.dt = '2025-01-25'
           AND dwd.stat_date = '2025-01-25'
     ) t
GROUP BY category_id, stat_date;

select * from dws_category_traffic_summary;
-- todo 4. dws_category_user_summary（品类人群汇总表）
CREATE TABLE dws_category_user_summary (
                                           category_id STRING COMMENT '品类ID',
                                           user_behavior STRING COMMENT '用户行为',
                                           user_portrait MAP<STRING, DOUBLE> COMMENT '人群画像（标签占比）',
                                           core_user_tags ARRAY<STRING> COMMENT '核心人群标签（top3）',
                                           stat_date STRING COMMENT '统计日期'
)
    PARTITIONED BY (dt STRING COMMENT '分区日期')
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS ORC;

-- 关闭统计信息自动收集，避免执行报错
SET hive.stats.autogather=false;

-- 从DWD层聚合数据至DWS层品类人群汇总表（分区dt='2025-01-25'）
INSERT INTO TABLE dws_category_user_summary PARTITION (dt='2025-01-25')
SELECT
    category_id,
    user_behavior,
    -- 人群画像（标签占比，手动构建map<string,double>）
    map(
            '20-25岁', SUM(CASE WHEN user_tag = '20-25岁' THEN tag_ratio ELSE 0 END),
            '26-30岁', SUM(CASE WHEN user_tag = '26-30岁' THEN tag_ratio ELSE 0 END),
            '31-35岁', SUM(CASE WHEN user_tag = '31-35岁' THEN tag_ratio ELSE 0 END),
            '男性', SUM(CASE WHEN user_tag = '男性' THEN tag_ratio ELSE 0 END),
            '女性', SUM(CASE WHEN user_tag = '女性' THEN tag_ratio ELSE 0 END),
            '一线城市', SUM(CASE WHEN user_tag = '一线城市' THEN tag_ratio ELSE 0 END),
            '二线城市', SUM(CASE WHEN user_tag = '二线城市' THEN tag_ratio ELSE 0 END)
    ) AS user_portrait,
    -- 核心人群标签（top3）
    array(
            MAX(CASE WHEN tag_rank = 1 THEN user_tag END),
            MAX(CASE WHEN tag_rank = 2 THEN user_tag END),
            MAX(CASE WHEN tag_rank = 3 THEN user_tag END)
    ) AS core_user_tags,
    stat_date
FROM (
         -- 计算标签占比和排名
         SELECT
             dwd.category_id,
             dwd.user_behavior,
             dwd.user_tag,
             dwd.stat_date,
             -- 标签在该行为人群中的占比
             ROUND(dwd.user_count / SUM(dwd.user_count) OVER (PARTITION BY dwd.category_id, dwd.user_behavior), 4) AS tag_ratio,
             -- 标签按占比排名
             ROW_NUMBER() OVER (
                 PARTITION BY dwd.category_id, dwd.user_behavior
                 ORDER BY dwd.user_count DESC
                 ) AS tag_rank
         FROM dwd_category_user_detail dwd
         WHERE dwd.dt = '2025-01-25'
           AND dwd.stat_date = '2025-01-25'
           AND dwd.user_behavior IN ('search', 'visit', 'pay')  -- 仅保留文档中指定的三种行为
     ) t
GROUP BY category_id, user_behavior, stat_date;

select * from dws_category_user_summary;