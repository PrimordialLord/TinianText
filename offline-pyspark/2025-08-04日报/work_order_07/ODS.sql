CREATE DATABASE IF NOT EXISTS work_order_07;
USE work_order_07;

-- ODS 商品基础信息表（ods_product_base）
DROP TABLE IF EXISTS ods_product_base;
CREATE TABLE IF NOT EXISTS ods_product_base
(
    product_id    STRING COMMENT '商品唯一标识',
    product_name  STRING COMMENT '商品名称',
    category_id   STRING COMMENT '商品分类ID',
    category_name STRING COMMENT '商品分类名称',
    brand_id      STRING COMMENT '品牌ID',
    brand_name    STRING COMMENT '品牌名称',
    price         DOUBLE COMMENT '商品售价',
    raw_data      STRING COMMENT '原始数据JSON',
    etl_time      STRING COMMENT 'ETL处理时间'
)
    COMMENT 'ODS层商品基础信息原始表'
    PARTITIONED BY (dt STRING COMMENT '数据日期')
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE;

-- 直接指定静态分区值（当前日期）
INSERT OVERWRITE TABLE ods_product_base PARTITION (dt = '2025-08-03') -- 使用具体日期替代变量
SELECT concat('prod_', lower(substr(md5(cast(rand() as string)), 1, 8))) AS product_id,
       concat(
           -- 简化随机前缀选择
               case floor(rand() * 7)
                   when 0 then '新款'
                   when 1 then '精选'
                   when 2 then '优质'
                   when 3 then '高级'
                   when 4 then '特惠'
                   when 5 then '限量版'
                   else '经典'
                   end, ' ',
               c.category_name, ' ',
               case c.category_name
                   when '电子产品' then
                       case floor(rand() * 5)
                           when 0 then '手机'
                           when 1 then '电脑'
                           when 2 then '平板'
                           when 3 then '耳机'
                           else '手表'
                           end
                   when '服装鞋帽' then
                       case floor(rand() * 5)
                           when 0 then 'T恤'
                           when 1 then '牛仔裤'
                           when 2 then '运动鞋'
                           when 3 then '衬衫'
                           else '外套'
                           end
                   when '家居用品' then
                       case floor(rand() * 5)
                           when 0 then '沙发'
                           when 1 then '桌子'
                           when 2 then '椅子'
                           when 3 then '台灯'
                           else '被子'
                           end
                   when '食品饮料' then
                       case floor(rand() * 5)
                           when 0 then '饼干'
                           when 1 then '巧克力'
                           when 2 then '饮料'
                           when 3 then '水果'
                           else '面包'
                           end
                   else
                       case floor(rand() * 5)
                           when 0 then '口红'
                           when 1 then '面膜'
                           when 2 then '洗发水'
                           when 3 then '沐浴露'
                           else '面霜'
                           end
                   end, ' ',
               case floor(rand() * 5)
                   when 0 then '升级版'
                   when 1 then '超值装'
                   when 2 then '豪华款'
                   when 3 then '基础款'
                   else '专业版'
                   end
       )                                                                 AS product_name,
       c.category_id,
       c.category_name,
       b.brand_id,
       b.brand_name,
       round(9.9 + rand() * (9999.9 - 9.9), 2)                           AS price,
       concat(
               '{"product_id":"', concat('prod_', lower(substr(md5(cast(rand() as string)), 1, 8))), '",',
               '"product_name":"', concat(
                       case floor(rand() * 7)
                           when 0 then '新款'
                           when 1 then '精选'
                           else '优质'
                           end, ' ',
                       c.category_name, ' ',
                       case c.category_name
                           when '电子产品' then
                               case floor(rand() * 5)
                                   when 0 then '手机'
                                   else '电脑'
                                   end
                           else '通用商品'
                           end
                                   ), '",',
           -- 关键修改：将BIGINT转换为INT类型
               '"create_time":"', date_sub(current_date(), cast(floor(rand() * 365) as int)), ' ',
               lpad(floor(rand() * 24), 2, '0'), ':',
               lpad(floor(rand() * 60), 2, '0'), ':',
               lpad(floor(rand() * 60), 2, '0'), '",',
               '"status":"',
               case floor(rand() * 3)
                   when 0 then '在售'
                   when 1 then '下架'
                   else '预售'
                   end, '"',
               '}'
       )                                                                 AS raw_data,
       from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss')            AS etl_time
FROM (SELECT 1 AS dummy) t
         CROSS JOIN (SELECT posexplode(split(space(99), '')) AS (id, val)) nums -- 生成100条数据
         CROSS JOIN (SELECT category_id, category_name
                     FROM (SELECT stack(5,
                                        'cat_101', '电子产品',
                                        'cat_201', '服装鞋帽',
                                        'cat_301', '家居用品',
                                        'cat_401', '食品饮料',
                                        'cat_501', '美妆个护'
                                  ) AS (category_id, category_name)) categories
                     ORDER BY rand()
                     LIMIT 1) c
         CROSS JOIN (SELECT brand_id, brand_name
                     FROM (SELECT stack(7,
                                        'brand_11', '品牌A',
                                        'brand_12', '品牌B',
                                        'brand_13', '品牌C',
                                        'brand_14', '品牌D',
                                        'brand_15', '品牌E',
                                        'brand_16', '品牌F',
                                        'brand_17', '品牌G'
                                  ) AS (brand_id, brand_name)) brands
                     ORDER BY rand()
                     LIMIT 1) b;

select *
from ods_product_base;

-- 商品行为数据表（ods_product_behavior）
DROP TABLE IF EXISTS ods_product_behavior;
CREATE TABLE IF NOT EXISTS ods_product_behavior
(
    product_id         STRING COMMENT '商品ID',
    visitor_count      BIGINT COMMENT '访客数',
    sales_amount       DOUBLE COMMENT '销售金额',
    sales_volume       BIGINT COMMENT '销量',
    conversion_rate    DOUBLE COMMENT '转化率',
    new_customer_count BIGINT COMMENT '新增客户数',
    traffic_index      DOUBLE COMMENT '流量指标原始值',
    conversion_index   DOUBLE COMMENT '转化指标原始值',
    content_index      DOUBLE COMMENT '内容指标原始值',
    new_customer_index DOUBLE COMMENT '拉新指标原始值',
    service_index      DOUBLE COMMENT '服务指标原始值',
    raw_data           STRING COMMENT '原始数据JSON',
    etl_time           STRING COMMENT 'ETL处理时间'
)
    COMMENT 'ODS层商品行为原始数据表'
    PARTITIONED BY (dt STRING COMMENT '数据日期')
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE;

-- 为ods_product_behavior表生成模拟数据（静态分区）
INSERT OVERWRITE TABLE ods_product_behavior PARTITION (dt = '2025-08-03')
SELECT
    -- 商品ID（与商品基础表关联逻辑，随机生成prod_前缀的8位ID）
    concat('prod_', lower(substr(md5(cast(rand() as string)), 1, 8))) AS product_id,

    -- 访客数（10-10000随机整数）
    cast(floor(10 + rand() * 9990) as bigint)                         AS visitor_count,

    -- 销售金额（0-100000随机数值，保留两位小数）
    round(rand() * 100000, 2)                                         AS sales_amount,

    -- 销量（0-1000随机整数，与销售金额正相关）
    cast(floor(
            case
                when rand() > 0.3 then rand() * 1000 -- 70%概率有销量
                else 0 -- 30%概率无销量
                end
         ) as bigint)                                                 AS sales_volume,

    -- 转化率（0-50%，保留4位小数）
    round(
            case
                when floor(10 + rand() * 9990) > 0 -- 避免除数为0
                    then (floor(rand() * 1000) / floor(10 + rand() * 9990)) * 100
                else 0
                end, 4
    )                                                                 AS conversion_rate,

    -- 新增客户数（0-500随机整数）
    cast(floor(rand() * 500) as bigint)                               AS new_customer_count,

    -- 流量指标原始值（0-100随机数值）
    round(rand() * 100, 2)                                            AS traffic_index,

    -- 转化指标原始值（0-100随机数值）
    round(rand() * 100, 2)                                            AS conversion_index,

    -- 内容指标原始值（0-100随机数值）
    round(rand() * 100, 2)                                            AS content_index,

    -- 拉新指标原始值（0-100随机数值）
    round(rand() * 100, 2)                                            AS new_customer_index,

    -- 服务指标原始值（0-100随机数值）
    round(rand() * 100, 2)                                            AS service_index,

    -- 原始JSON数据（包含所有字段的原始信息）
    concat(
            '{"product_id":"', concat('prod_', lower(substr(md5(cast(rand() as string)), 1, 8))), '",',
            '"visitor_count":', cast(floor(10 + rand() * 9990) as string), ',',
            '"sales_amount":', round(rand() * 100000, 2), ',',
            '"sales_volume":', cast(floor(rand() * 1000) as string), ',',
            '"conversion_rate":', round(rand() * 50, 4), ',',
            '"new_customer_count":', cast(floor(rand() * 500) as string), ',',
            '"metrics":{',
            '"traffic":', round(rand() * 100, 2), ',',
            '"conversion":', round(rand() * 100, 2), ',',
            '"content":', round(rand() * 100, 2), ',',
            '"new_customer":', round(rand() * 100, 2), ',',
            '"service":', round(rand() * 100, 2), ''
                '}},',
            '"collect_time":"', current_date(), ' ',
            lpad(floor(rand() * 24), 2, '0'), ':',
            lpad(floor(rand() * 60), 2, '0'), ':',
            lpad(floor(rand() * 60), 2, '0'), '"'
    )                                                                 AS raw_data,

    -- ETL处理时间（当前时间）
    from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss')            AS etl_time
FROM
    -- 基础表生成100条数据（可调整space参数控制数量）
    (SELECT posexplode(split(space(99), '')) AS (id, val)) nums;

select *
from ods_product_behavior;

-- ODS 竞品对比数据表（ods_competitor_info）
CREATE TABLE IF NOT EXISTS ods_competitor_info
(
    product_id            STRING COMMENT '本品ID',
    competitor_product_id STRING COMMENT '竞品ID',
    dimension             STRING COMMENT '对比维度',
    self_value            DOUBLE COMMENT '本品指标值',
    competitor_value      DOUBLE COMMENT '竞品指标值',
    raw_data              STRING COMMENT '原始数据JSON',
    etl_time              STRING COMMENT 'ETL处理时间'
)
    COMMENT 'ODS层竞品对比原始数据表'
    PARTITIONED BY (dt STRING COMMENT '数据日期')
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE;

-- 为竞品对比表生成模拟数据（静态分区，确保兼容性）
INSERT OVERWRITE TABLE ods_competitor_info PARTITION (dt = '2025-08-03')
SELECT
    -- 本品ID（与商品表关联，格式统一）
    concat('prod_', lower(substr(md5(cast(rand() as string)), 1, 8)))           AS product_id,

    -- 竞品ID（独立格式，区分本品）
    concat('comp_', lower(substr(md5(cast(rand() * 1000000 as string)), 1, 8))) AS competitor_product_id,

    -- 对比维度（随机选择常见竞品分析维度）
    case floor(rand() * 8)
        when 0 then '价格'
        when 1 then '销量'
        when 2 then '好评率'
        when 3 then '功能完整性'
        when 4 then '售后服务'
        when 5 then '市场占有率'
        when 6 then '用户复购率'
        else '品牌知名度'
        end                                                                     AS dimension,

    -- 本品指标值（根据维度生成合理范围）
    case floor(rand() * 8)
        when 0 then round(50 + rand() * 9950, 2) -- 价格：50-10000
        when 1 then round(10 + rand() * 9990, 0) -- 销量：10-10000
        when 2 then round(80 + rand() * 20, 2) -- 好评率：80%-100%
        when 3 then round(3 + rand() * 7, 1) -- 功能完整性：3-10分
        when 4 then round(3 + rand() * 7, 1) -- 售后服务：3-10分
        when 5 then round(5 + rand() * 30, 2) -- 市场占有率：5%-35%
        when 6 then round(10 + rand() * 50, 2) -- 用户复购率：10%-60%
        else round(40 + rand() * 60, 2) -- 品牌知名度：40-100分
        end                                                                     AS self_value,

    -- 竞品指标值（与本品有差异，模拟竞争关系）
    case floor(rand() * 8)
        when 0 then round(
                (50 + rand() * 9950) * (0.8 + rand() * 0.4) -- 价格：本品的80%-120%
            , 2)
        when 1 then round(
                (10 + rand() * 9990) * (0.5 + rand() * 1) -- 销量：本品的50%-150%
            , 0)
        when 2 then round(
                (80 + rand() * 20) * (0.8 + rand() * 0.4) -- 好评率：本品的80%-120%
            , 2)
        else round(
                (3 + rand() * 7) * (0.7 + rand() * 0.6) -- 其他指标：本品的70%-130%
            , 1)
        end                                                                     AS competitor_value,

    -- 原始JSON数据（包含完整对比信息）
    concat(
            '{"product_id":"', concat('prod_', lower(substr(md5(cast(rand() as string)), 1, 8))), '",',
            '"competitor_product_id":"', concat('comp_', lower(substr(md5(cast(rand() * 1000000 as string)), 1, 8))),
            '",',
            '"dimension":"', case floor(rand() * 8)
                                 when 0 then '价格'
                                 when 1 then '销量'
                                 else '好评率'
                end, '",',
            '"self_value":', case floor(rand() * 8)
                                 when 0 then round(50 + rand() * 9950, 2)
                                 else round(80 + rand() * 20, 2)
                end, ',',
            '"competitor_value":', case floor(rand() * 8)
                                       when 0 then round((50 + rand() * 9950) * (0.8 + rand() * 0.4), 2)
                                       else round((80 + rand() * 20) * (0.8 + rand() * 0.4), 2)
                end, ',',
            '"compare_result":"', case
                                      when (case floor(rand() * 8)
                                                when 0 then 50 + rand() * 9950
                                                else 80 + rand() * 20 end)
                                          > (case floor(rand() * 8)
                                                 when 0 then (50 + rand() * 9950) * (0.8 + rand() * 0.4)
                                                 else (80 + rand() * 20) * (0.8 + rand() * 0.4) end)
                                          then '本品占优'
                                      else '竞品占优'
                end, '",',
            '"collect_time":"', current_date(), ' ',
            lpad(floor(rand() * 24), 2, '0'), ':',
            lpad(floor(rand() * 60), 2, '0'), ':',
            lpad(floor(rand() * 60), 2, '0'), '"'
    )                                                                           AS raw_data,

    -- ETL处理时间（当前时间）
    from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss')                      AS etl_time
FROM
    -- 生成200条数据（可调整space参数控制数量）
    (SELECT posexplode(split(space(199), '')) AS (id, val)) nums;

select *
from ods_competitor_info;
