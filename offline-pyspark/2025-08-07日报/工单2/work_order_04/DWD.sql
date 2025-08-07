Use work_order_04_double;

-- TODO 1. dwd_category_sales_detail（品类销售详情表）
CREATE TABLE dwd_category_sales_detail (
                                           category_id STRING COMMENT '品类ID',
                                           category_name STRING COMMENT '品类名称',
                                           daily_sales_amount DOUBLE COMMENT '当日销售额',
                                           daily_sales_num INT COMMENT '当日销量',
                                           monthly_target_gmv DOUBLE COMMENT '月目标GMV',
                                           stat_date STRING COMMENT '统计日期'
)
    PARTITIONED BY (dt STRING COMMENT '分区日期')
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS ORC;

-- 关闭统计信息自动收集，避免执行报错
SET hive.stats.autogather=false;

-- 从ODS层抽取并清洗数据至DWD层品类销售详情表（分区dt='2025-01-25'）
INSERT INTO TABLE dwd_category_sales_detail PARTITION (dt='2025-01-25')
SELECT
    ods.category_id,
    ods.category_name,
    SUM(ods.sales_amount) AS daily_sales_amount,
    SUM(ods.sales_num) AS daily_sales_num,
    -- 按品类类型生成月目标GMV，匹配文档中“设定类目目标”的需求
    CASE
        WHEN ods.category_name IN ('家电', '数码') THEN ROUND(SUM(ods.sales_amount) * 30 * 1.2, 2)  -- 高目标品类
        WHEN ods.category_name IN ('男装', '女装') THEN ROUND(SUM(ods.sales_amount) * 30 * 0.9, 2)   -- 中目标品类
        ELSE ROUND(SUM(ods.sales_amount) * 30 * 0.7, 2)  -- 低目标品类
        END AS monthly_target_gmv,
    ods.pay_date AS stat_date
FROM ods_category_sales_log ods
WHERE ods.dt = '2025-01-25'
  AND ods.pay_date = '2025-01-25'
  AND ods.category_id IS NOT NULL
  AND ods.category_name IS NOT NULL
GROUP BY ods.category_id, ods.category_name, ods.pay_date;

select * from dwd_category_sales_detail;

-- todo 2. dwd_category_property_detail（品类属性详情表）
CREATE TABLE dwd_category_property_detail (
                                              category_id STRING COMMENT '品类ID',
                                              property_name STRING COMMENT '属性名称',
                                              property_value STRING COMMENT '属性值',
                                              traffic_num INT COMMENT '属性流量',
                                              conversion_rate DOUBLE COMMENT '支付转化率',
                                              product_count INT COMMENT '动销商品数',
                                              stat_date STRING COMMENT '统计日期'
)
    PARTITIONED BY (dt STRING COMMENT '分区日期')
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS ORC;

-- 关闭统计信息自动收集，避免执行报错
SET hive.stats.autogather=false;

-- 向品类属性详情表插入数据（分区dt='2025-01-25'）
INSERT INTO TABLE dwd_category_property_detail PARTITION (dt='2025-01-25')
SELECT
    -- 品类ID（覆盖文档中多品类属性分析场景）
    CASE
        WHEN prop_group = 1 THEN 'C001'  -- 男装
        WHEN prop_group = 2 THEN 'C002'  -- 女装
        WHEN prop_group = 3 THEN 'C007'  -- 家电
        WHEN prop_group = 4 THEN 'C008'  -- 数码
        ELSE 'C005'  -- 箱包
        END,
    -- 属性名称（按品类分组，符合文档中“属性对销售影响分析”需求）
    CASE
        WHEN prop_group IN (1,2) THEN '颜色'  -- 服饰类属性
        WHEN prop_group IN (3,4) THEN '规格'  -- 家电数码类属性
        ELSE '材质'  -- 箱包类属性
        END,
    -- 属性值（对应属性名称，支持属性效果对比）
    CASE
        WHEN prop_group IN (1,2) THEN
            CASE CEIL(rand()*5)
                WHEN 1 THEN '黑色' WHEN 2 THEN '白色' WHEN 3 THEN '蓝色'
                WHEN 4 THEN '红色' ELSE '灰色'
                END
        WHEN prop_group IN (3,4) THEN
            CASE CEIL(rand()*3)
                WHEN 1 THEN '标准版' WHEN 2 THEN '进阶版' ELSE '旗舰版'
                END
        ELSE
            CASE CEIL(rand()*3)
                WHEN 1 THEN '真皮' WHEN 2 THEN '帆布' ELSE 'PU'
                END
        END,
    -- 属性流量（100-5000，满足文档中流量分析需求）
    CAST(rand()*4900 + 100 AS INT),
    -- 支付转化率（1%-30%，用于评估属性转化效果）
    ROUND(rand()*0.29 + 0.01, 4),
    -- 动销商品数（5-50，支持文档中“动销商品数分析”）
    CAST(rand()*45 + 5 AS INT),
    -- 统计日期（与分区一致，符合时间维度要求）
    '2025-01-25'
FROM (
         -- 生成属性分组（1-5），控制数据分布
         SELECT CEIL(rand()*5) AS prop_group
         FROM (
                  -- 生成100条数据（通过posexplode简化逻辑）
                  SELECT pos FROM (SELECT posexplode(split(space(99), ' ')) AS (pos, val)) t
              ) t_gen
     ) t;

select * from  dwd_category_property_detail;
-- todo 3. dwd_category_traffic_detail（品类流量详情表）
CREATE TABLE dwd_category_traffic_detail (
                                             category_id STRING COMMENT '品类ID',
                                             channel STRING COMMENT '流量渠道',
                                             visitor_num INT COMMENT '渠道访客数',
                                             search_word STRING COMMENT '热搜词',
                                             search_visitor_num INT COMMENT '搜索词访客量',
                                             stat_date STRING COMMENT '统计日期'
)
    PARTITIONED BY (dt STRING COMMENT '分区日期')
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS ORC;

-- 关闭统计信息自动收集，避免执行报错
SET hive.stats.autogather=false;

-- 从ODS层抽取并清洗数据至DWD层品类流量详情表（分区dt='2025-01-25'）
INSERT INTO TABLE dwd_category_traffic_detail PARTITION (dt='2025-01-25')
SELECT
    -- 品类ID（保留ODS层核心标识，支持文档中多品类流量对比分析）
    ods.category_id,
    -- 流量渠道（筛选有效渠道，匹配文档中“渠道分布分析”需求）
    CASE
        WHEN ods.channel IN ('手淘搜索', '直通车', '短视频', '直播', '推荐位') THEN ods.channel
        ELSE 'other'  -- 归类异常渠道，确保数据规范性
        END AS channel,
    -- 渠道访客数（汇总计算，支撑文档中渠道流量规模分析）
    SUM(ods.visitor_num) AS visitor_num,
    -- 热搜词（清洗格式，符合文档中“热搜词访客量分析”要求）
    TRIM(ods.search_word),
    -- 搜索词访客量（汇总计算，用于评估热搜词效果）
    SUM(ods.search_visitor_num) AS search_visitor_num,
    -- 统计日期（与分区一致，满足文档中按月查看的时间维度要求）
    ods.stat_date
FROM ods_category_traffic_log ods  -- ODS层源表，提供原始流量数据
WHERE ods.dt = '2025-01-25'  -- 抽取当日数据，匹配时间维度分析场景
  AND ods.stat_date = '2025-01-25'
  AND ods.category_id IS NOT NULL  -- 过滤无效品类ID，保证数据质量
GROUP BY ods.category_id, ods.channel, TRIM(ods.search_word), ods.stat_date;

select * from dwd_category_traffic_detail;
-- todo 4. dwd_category_user_detail（品类人群详情表）
CREATE TABLE dwd_category_user_detail (
                                          category_id STRING COMMENT '品类ID',
                                          user_behavior STRING COMMENT '用户行为（search/visit/pay）',
                                          user_tag STRING COMMENT '人群标签',
                                          user_count INT COMMENT '人群数量',
                                          stat_date STRING COMMENT '统计日期'
)
    PARTITIONED BY (dt STRING COMMENT '分区日期')
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS ORC;

-- 关闭统计信息自动收集，避免执行报错
SET hive.stats.autogather=false;

-- 从ODS层抽取并清洗数据至DWD层品类人群详情表（分区dt='2025-01-25'）
INSERT INTO TABLE dwd_category_user_detail PARTITION (dt='2025-01-25')
SELECT
    -- 品类ID（保留ODS层核心标识，支持文档中多品类人群分析）
    ods.category_id,
    -- 用户行为（筛选文档中指定的三类行为，确保数据有效性）
    CASE
        WHEN ods.user_behavior IN ('search', 'visit', 'pay') THEN ods.user_behavior
        ELSE 'unknown'  -- 处理异常值，符合文档中人群行为分析的规范性要求
        END AS user_behavior,
    -- 人群标签（清洗格式，支撑文档中人群画像特征提取）
    TRIM(ods.user_tag),  -- 去除冗余空格，优化标签格式
    -- 人群数量（汇总计算，满足文档中人群规模统计需求）
    SUM(ods.user_count) AS user_count,
    -- 统计日期（与分区一致，符合文档中按月查看的时间维度要求）
    ods.stat_date
FROM ods_category_user_log ods  -- ODS层源表，为DWD层提供原始数据
WHERE ods.dt = '2025-01-25'  -- 抽取当日数据，匹配文档中时间维度分析场景
  AND ods.stat_date = '2025-01-25'
  AND ods.category_id IS NOT NULL  -- 过滤无效品类ID，保证数据质量
GROUP BY ods.category_id, ods.user_behavior, TRIM(ods.user_tag), ods.stat_date;

select * from dwd_category_user_detail;