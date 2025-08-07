CREATE DATABASE IF NOT EXISTS work_order_04_double;

USE work_order_04_double;

-- TODO 1. ods_category_sales_log（品类销售日志表）
CREATE TABLE ods_category_sales_log
(
    category_id   STRING COMMENT '品类ID',
    category_name STRING COMMENT '品类名称',
    sales_amount  DOUBLE COMMENT '销售额',
    sales_num     INT COMMENT '销量',
    pay_date      STRING COMMENT '支付日期',
    create_time   STRING COMMENT '日志生成时间'
)
    PARTITIONED BY (dt STRING COMMENT '分区日期')
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE;

-- 关闭统计信息自动收集（避免StatsTask执行失败）
SET hive.stats.autogather=false;

-- 生成2025-01-25分区数据（180条），适配品类360看板销售分析场景
INSERT INTO TABLE ods_category_sales_log PARTITION (dt='2025-01-25')
SELECT
    -- 品类ID（C001-C010，支持文档中多品类分析）
    CONCAT('C', LPAD(CAST(ceil(rand() * 10) AS INT), 3, '0')),
    -- 品类名称（匹配文档中销售分析的品类维度）
    CASE
        WHEN ceil(rand() * 10) = 1 THEN '男装'
        WHEN ceil(rand() * 10) = 2 THEN '女装'
        WHEN ceil(rand() * 10) = 3 THEN '童装'
        WHEN ceil(rand() * 10) = 4 THEN '鞋靴'
        WHEN ceil(rand() * 10) = 5 THEN '箱包'
        WHEN ceil(rand() * 10) = 6 THEN '美妆'
        WHEN ceil(rand() * 10) = 7 THEN '家电'
        WHEN ceil(rand() * 10) = 8 THEN '数码'
        WHEN ceil(rand() * 10) = 9 THEN '食品'
        ELSE '家居'
        END,
    -- 销售额（支持文档中总销售额统计需求）
    ROUND(rand() * 99000 + 1000, 2),
    -- 销量（用于文档中销量趋势分析）
    CAST(rand() * 990 + 10 AS INT),
    -- 支付日期（与分区一致，满足日维度分析）
    '2025-01-25',
    -- 日志时间（模拟真实交易时间分布）
    CONCAT('2025-01-25 ',
           LPAD(CAST(rand() * 23 AS INT), 2, '0'), ':',
           LPAD(CAST(rand() * 59 AS INT), 2, '0'), ':',
           LPAD(CAST(rand() * 59 AS INT), 2, '0'))
FROM (
         -- 简化数据生成逻辑，避免JOIN过多导致的任务异常
         SELECT pos AS id FROM (SELECT posexplode(split(space(179), ' ')) AS (pos, val)) t
     ) t_gen;

select * from ods_category_sales_log;
-- TODO 2. ods_category_property_log（品类属性日志表）
CREATE TABLE ods_category_property_log
(
    category_id     STRING COMMENT '品类ID',
    property_name   STRING COMMENT '属性名称',
    property_value  STRING COMMENT '属性值',
    traffic_num     INT COMMENT '属性带来的流量',
    conversion_rate DOUBLE COMMENT '支付转化率',
    product_count   INT COMMENT '动销商品数',
    stat_date       STRING COMMENT '统计日期'
)
    PARTITIONED BY (dt STRING COMMENT '分区日期')
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE;

-- 关闭统计信息自动收集，避免执行报错
SET hive.stats.autogather=false;

-- 向品类属性日志表插入模拟数据（分区dt='2025-01-25'）
INSERT INTO TABLE ods_category_property_log PARTITION (dt='2025-01-25')
SELECT
    -- 品类ID（关联文档中品类分析的多品类场景）
    CASE
        WHEN category_group = 1 THEN 'C001'  -- 男装
        WHEN category_group = 2 THEN 'C002'  -- 女装
        WHEN category_group = 3 THEN 'C007'  -- 家电
        WHEN category_group = 4 THEN 'C008'  -- 数码
        ELSE 'C005'  -- 箱包
        END,
    -- 属性名称（按品类分组，符合文档中“属性对销售影响分析”需求）
    CASE
        WHEN category_group IN (1,2) THEN '颜色'  -- 服饰类属性
        WHEN category_group IN (3,4) THEN '规格'  -- 家电数码类属性
        ELSE '材质'  -- 箱包类属性
        END,
    -- 属性值（对应属性名称，支持属性效果对比）
    CASE
        WHEN category_group IN (1,2) THEN
            CASE CEIL(rand()*5)
                WHEN 1 THEN '黑色' WHEN 2 THEN '白色' WHEN 3 THEN '蓝色'
                WHEN 4 THEN '红色' ELSE '灰色'
                END
        WHEN category_group IN (3,4) THEN
            CASE CEIL(rand()*3)
                WHEN 1 THEN '标准版' WHEN 2 THEN '进阶版' ELSE '旗舰版'
                END
        ELSE
            CASE CEIL(rand()*3)
                WHEN 1 THEN '真皮' WHEN 2 THEN '帆布' ELSE 'PU'
                END
        END,
    -- 属性带来的流量（100-5000，满足文档中流量分析需求）
    CAST(rand()*4900 + 100 AS INT),
    -- 支付转化率（1%-30%，用于评估属性转化效果）
    ROUND(rand()*0.29 + 0.01, 4),
    -- 动销商品数（5-50，支持文档中“动销商品数分析”）
    CAST(rand()*45 + 5 AS INT),
    -- 统计日期（与分区一致，符合时间维度要求）
    '2025-01-25'
FROM (
         -- 生成品类分组（1-5），控制数据分布
         SELECT CEIL(rand()*5) AS category_group
         FROM (
                  -- 生成100条数据（通过posexplode简化逻辑）
                  SELECT pos FROM (SELECT posexplode(split(space(99), ' ')) AS (pos, val)) t
              ) t_gen
     ) t;

select * from ods_category_property_log;
-- TODO 3. ods_category_traffic_log（品类流量日志表）
CREATE TABLE ods_category_traffic_log
(
    category_id        STRING COMMENT '品类ID',
    channel            STRING COMMENT '流量渠道',
    visitor_num        INT COMMENT '访客数',
    search_word        STRING COMMENT '热搜词',
    search_visitor_num INT COMMENT '搜索词访客量',
    stat_date          STRING COMMENT '统计日期'
)
    PARTITIONED BY (dt STRING COMMENT '分区日期')
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE;

-- 关闭统计信息自动收集，避免执行报错
SET hive.stats.autogather=false;

-- 向品类流量日志表插入模拟数据（分区dt='2025-01-25'）
INSERT INTO TABLE ods_category_traffic_log PARTITION (dt='2025-01-25')
SELECT
    -- 品类ID（覆盖文档中多品类流量分析场景）
    CONCAT('C', LPAD(CAST(ceil(rand() * 10) AS INT), 3, '0')),
    -- 流量渠道（匹配文档中“渠道分布分析”需求）
    CASE CEIL(rand() * 5)
        WHEN 1 THEN '手淘搜索'
        WHEN 2 THEN '直通车'
        WHEN 3 THEN '短视频'
        WHEN 4 THEN '直播'
        ELSE '推荐位'
        END,
    -- 访客数（100-10000，支持文档中渠道流量规模分析）
    CAST(rand() * 9900 + 100 AS INT),
    -- 热搜词（对应品类特征，满足文档中“热搜词访客量分析”）
    CASE
        WHEN category_id LIKE 'C001%' THEN '男士外套 新款'
        WHEN category_id LIKE 'C002%' THEN '女装连衣裙 春季'
        WHEN category_id LIKE 'C007%' THEN '家用冰箱 节能'
        WHEN category_id LIKE 'C008%' THEN '智能手机 大内存'
        ELSE '时尚箱包 百搭'
        END,
    -- 搜索词访客量（50-5000，与访客数成合理比例）
    CAST(rand() * 4950 + 50 AS INT),
    -- 统计日期（与分区一致，符合时间维度要求）
    '2025-01-25'
FROM (
         -- 生成品类ID基础数据
         SELECT CONCAT('C', LPAD(CAST(ceil(rand() * 10) AS INT), 3, '0')) AS category_id
         FROM (
                  -- 生成120条数据（通过posexplode简化逻辑）
                  SELECT pos FROM (SELECT posexplode(split(space(119), ' ')) AS (pos, val)) t
              ) t_gen
     ) t;

select * from ods_category_traffic_log;
-- TODO 4. ods_category_user_log（品类人群日志表）
CREATE TABLE ods_category_user_log
(
    category_id   STRING COMMENT '品类ID',
    user_behavior STRING COMMENT '用户行为（search/visit/pay）',
    user_tag      STRING COMMENT '人群标签（如年龄、性别、地域等）',
    user_count    INT COMMENT '人群数量',
    stat_date     STRING COMMENT '统计日期'
)
    PARTITIONED BY (dt STRING COMMENT '分区日期')
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE;

-- 关闭统计信息自动收集，避免执行报错
SET hive.stats.autogather=false;

-- 向品类人群日志表插入模拟数据（分区dt='2025-01-25'）
INSERT INTO TABLE ods_category_user_log PARTITION (dt='2025-01-25')
SELECT
    -- 品类ID（覆盖文档中多品类人群分析场景）
    CONCAT('C', LPAD(CAST(ceil(rand() * 10) AS INT), 3, '0')),
    -- 用户行为（匹配文档中“搜索/访问/支付人群”分析需求）
    CASE CEIL(rand() * 3)
        WHEN 1 THEN 'search'  -- 搜索人群
        WHEN 2 THEN 'visit'   -- 访问人群
        ELSE 'pay'            -- 支付人群
        END,
    -- 人群标签（包含年龄、性别、地域，支持文档中人群画像分析）
    CONCAT(
            '年龄:', CASE CEIL(rand() * 4)
                         WHEN 1 THEN '18-25' WHEN 2 THEN '26-35' WHEN 3 THEN '36-45' ELSE '46+' END,
            ',性别:', CASE CEIL(rand() * 2) WHEN 1 THEN '男' ELSE '女' END,
            ',地域:', CASE CEIL(rand() * 5)
                          WHEN 1 THEN '一线' WHEN 2 THEN '二线' WHEN 3 THEN '三线' WHEN 4 THEN '四线' ELSE '五线' END
    ),
    -- 人群数量（100-5000，符合文档中人群规模分析需求）
    CAST(rand() * 4900 + 100 AS INT),
    -- 统计日期（与分区一致，满足时间维度要求）
    '2025-01-25'
FROM (
         -- 生成150条数据（通过posexplode简化逻辑）
         SELECT pos FROM (SELECT posexplode(split(space(149), ' ')) AS (pos, val)) t
     ) t_gen;

select * from ods_category_user_log;