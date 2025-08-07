USE work_order_03_double;

-- 商品核心数据日志（支持核心概况分析）
CREATE EXTERNAL TABLE ods_product_core_log (
                                               id STRING COMMENT '记录ID',
                                               product_id STRING COMMENT '商品ID',
                                               click_num INT COMMENT '点击量',
                                               sales_num INT COMMENT '销量',
                                               sales_amount DOUBLE COMMENT '销售额',
                                               op_action STRING COMMENT '运营行动（如报名新客折扣、发布短视频）',
                                               create_time STRING COMMENT '创建时间'
)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

-- 向商品核心数据日志表插入模拟数据，适配商品360看板核心概况分析需求
INSERT INTO TABLE ods_product_core_log
SELECT
    -- 生成唯一记录ID（结合随机数与自增序列）
    CONCAT('core_', row_number() OVER ()) AS id,
    -- 商品ID：模拟1000-2000区间的商品（覆盖核心分析的单品范围）
    CAST(FLOOR(RAND() * 1000) + 1000 AS STRING) AS product_id,
    -- 点击量：分层模拟（10-1000次），符合不同商品热度
    CAST(CASE
             WHEN RAND() > 0.8 THEN RAND() * 900 + 100  -- 20%商品点击量100-1000
             WHEN RAND() > 0.3 THEN RAND() * 90 + 10    -- 50%商品点击量10-100
             ELSE RAND() * 9 + 1                       -- 30%商品点击量1-10
        END AS INT) AS click_num,
    -- 销量：与点击量正相关（点击量越高，销量概率越大）
    CAST(CASE
             WHEN RAND() > 0.7 THEN RAND() * 50 + 10    -- 30%商品销量10-60
             WHEN RAND() > 0.2 THEN RAND() * 9 + 1      -- 50%商品销量1-10
             ELSE 0                                     -- 20%商品无销量
        END AS INT) AS sales_num,
    -- 销售额：销量×随机单价（50-500元）
    CAST(
            (CASE
                 WHEN RAND() > 0.7 THEN RAND() * 50 + 10
                 WHEN RAND() > 0.2 THEN RAND() * 9 + 1
                 ELSE 0
                END) * (RAND() * 450 + 50)
        AS DOUBLE
    ) AS sales_amount,
    -- 运营行动：随机模拟常见运营动作（对应文档中运营节点支持需求）
    CASE
        WHEN RAND() > 0.7 THEN '报名新客折扣'
        WHEN RAND() > 0.4 THEN '发布短视频'
        WHEN RAND() > 0.1 THEN '修改手淘推荐主图'
        ELSE '无操作'
        END AS op_action,
    -- 创建时间：近30天内的随机时间（精确到秒）
    FROM_UNIXTIME(
            UNIX_TIMESTAMP(DATE_ADD(CURRENT_DATE, -CAST(FLOOR(RAND() * 30) AS INT)))
                + CAST(FLOOR(RAND() * 86400) AS INT)
    ) AS create_time
-- 生成1000条数据（通过多层UNION ALL实现，兼容低版本Hive）
FROM (
         SELECT 1 AS num UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1
         UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1
     ) t1 -- 10条
         CROSS JOIN (
    SELECT 1 AS num UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1
    UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1
) t2 -- 10×10=100条
         CROSS JOIN (
    SELECT 1 AS num UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1
    UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1
) t3; -- 100×10=1000条

select * from ods_product_core_log;
-- SKU销售日志（支持SKU销售详情/属性分析）
CREATE EXTERNAL TABLE ods_sku_sales_log (
                                            id STRING COMMENT '记录ID',
                                            sku_id STRING COMMENT 'SKU ID',
                                            product_id STRING COMMENT '商品ID',
                                            attribute STRING COMMENT '商品属性（如颜色、尺寸）',
                                            sales_num INT COMMENT '销量',
                                            create_time STRING COMMENT '创建时间'
)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

-- 向SKU销售日志表插入模拟数据，适配商品360看板SKU销售详情及属性分析需求（修正表别名引用错误）
INSERT INTO TABLE ods_sku_sales_log
SELECT
    -- 生成唯一记录ID
    CONCAT('sku_sales_', row_number() OVER ()) AS id,
    -- SKU ID：格式为商品ID+属性编码，确保唯一性（先定义商品ID变量再引用）
    CONCAT(CAST(FLOOR(RAND() * 1000) + 1000 AS STRING), '_', CAST(FLOOR(RAND() * 100) AS STRING)) AS sku_id,
    -- 商品ID：关联商品核心数据，范围1000-2000
    CAST(FLOOR(RAND() * 1000) + 1000 AS STRING) AS product_id,
    -- 商品属性：模拟颜色+尺寸组合（符合SKU属性分析场景）
    CONCAT(
            CASE FLOOR(RAND() * 5)
                WHEN 0 THEN '颜色:白色'
                WHEN 1 THEN '颜色:黑色'
                WHEN 2 THEN '颜色:红色'
                WHEN 3 THEN '颜色:蓝色'
                ELSE '颜色:灰色'
                END,
            ',',
            CASE FLOOR(RAND() * 4)
                WHEN 0 THEN '尺寸:S'
                WHEN 1 THEN '尺寸:M'
                WHEN 2 THEN '尺寸:L'
                ELSE '尺寸:XL'
                END
    ) AS attribute,
    -- 销量：分层模拟，热销SKU销量更高（支撑热销程度分析）
    CAST(CASE
             WHEN RAND() > 0.8 THEN RAND() * 190 + 10  -- 20%热销SKU：10-200件
             WHEN RAND() > 0.3 THEN RAND() * 9 + 1     -- 50%普通SKU：1-10件
             ELSE 0                                    -- 30%滞销SKU：0件
        END AS INT) AS sales_num,
    -- 创建时间：近30天内随机时间，与商品核心数据时间范围一致
    FROM_UNIXTIME(
            UNIX_TIMESTAMP(DATE_ADD(CURRENT_DATE, -CAST(FLOOR(RAND() * 30) AS INT)))
                + CAST(FLOOR(RAND() * 86400) AS INT)
    ) AS create_time
-- 生成1500条数据（覆盖多商品多SKU场景）
FROM (
         SELECT 1 AS num UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1
         UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1
     ) t1 -- 10条
         CROSS JOIN (
    SELECT 1 AS num UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1
    UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1
) t2 -- 10×10=100条
         CROSS JOIN (
    SELECT 1 AS num UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1
    UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1
    UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1
    UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1
    UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1
) t3; -- 100×15=1500条

select * from ods_sku_sales_log;
-- 价格日志（支持价格分析）
CREATE EXTERNAL TABLE ods_price_log (
                                        id STRING COMMENT '记录ID',
                                        product_id STRING COMMENT '商品ID',
                                        price DOUBLE COMMENT '商品价格',
                                        category_id STRING COMMENT '类目ID',
                                        price_time STRING COMMENT '价格记录时间'
)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

-- 向价格日志表插入模拟数据，适配商品360看板价格分析需求（价格趋势、价格带分布等）
INSERT INTO TABLE ods_price_log
SELECT
    -- 生成唯一记录ID
    CONCAT('price_', row_number() OVER ()) AS id,
    -- 商品ID：与商品核心数据、SKU销售数据关联，范围1000-2000
    CAST(FLOOR(RAND() * 1000) + 1000 AS STRING) AS product_id,
    -- 商品价格：分层模拟不同价格带（支撑价格带分析）
    CAST(CASE
             WHEN RAND() > 0.8 THEN RAND() * 500 + 500  -- 20%高价商品：500-1000元
             WHEN RAND() > 0.5 THEN RAND() * 300 + 200  -- 30%中高价位：200-500元
             WHEN RAND() > 0.2 THEN RAND() * 150 + 50   -- 30%中等价位：50-200元
             ELSE RAND() * 40 + 10                      -- 20%低价商品：10-50元
        END AS DOUBLE) AS price,
    -- 类目ID：模拟叶子类目，范围10-50（支撑类目价格带分布）
    CAST(FLOOR(RAND() * 40) + 10 AS STRING) AS category_id,
    -- 价格记录时间：近90天内随机时间（含价格变动历史，支撑趋势分析）
    FROM_UNIXTIME(
            UNIX_TIMESTAMP(DATE_ADD(CURRENT_DATE, -CAST(FLOOR(RAND() * 90) AS INT)))
                + CAST(FLOOR(RAND() * 86400) AS INT)
    ) AS price_time
-- 生成2000条数据（覆盖多商品多类目价格记录）
FROM (
         SELECT 1 AS num UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1
         UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1
     ) t1 -- 10条
         CROSS JOIN (
    SELECT 1 AS num UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1
    UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1
) t2 -- 10×10=100条
         CROSS JOIN (
    SELECT 1 AS num UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1
    UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1
    UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1
    UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1
    UNION ALL SELECT 1 UNION ALL SELECT 1
) t3; -- 100×20=2000条

select * from ods_price_log;
-- 流量来源日志（支持流量来源分析）
CREATE EXTERNAL TABLE ods_traffic_source_log (
                                                 id STRING COMMENT '记录ID',
                                                 product_id STRING COMMENT '商品ID',
                                                 channel STRING COMMENT '流量渠道',
                                                 visitor_num INT COMMENT '访客数',
                                                 conversion_num INT COMMENT '转化数',
                                                 log_time STRING COMMENT '日志时间'
)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

-- 向流量来源日志表插入模拟数据，适配商品360看板流量来源分析需求（修正访客数字段引用错误）
INSERT INTO TABLE ods_traffic_source_log
SELECT
    -- 生成唯一记录ID
    CONCAT('traffic_', row_number() OVER ()) AS id,
    -- 商品ID：与商品核心数据、SKU销售数据关联，范围1000-2000
    CAST(FLOOR(RAND() * 1000) + 1000 AS STRING) AS product_id,
    -- 流量渠道：使用子查询定义的channel字段
    channel,
    -- 访客数：基于渠道类型分层模拟流量规模，先定义为变量
    visitor_num,
    -- 转化数：基于访客数和渠道类型计算，使用已定义的visitor_num
    CAST(CASE
             WHEN channel = '直通车' THEN visitor_num * (RAND() * 0.1 + 0.05)  -- 直通车转化率较高：5%-15%
             WHEN channel = '聚划算' THEN visitor_num * (RAND() * 0.08 + 0.03) -- 聚划算：3%-11%
             ELSE visitor_num * (RAND() * 0.05 + 0.01)                          -- 其他渠道：1%-6%
        END AS INT) AS conversion_num,
    -- 日志时间：近30天内随机时间，与其他表时间范围一致
    FROM_UNIXTIME(
            UNIX_TIMESTAMP(DATE_ADD(CURRENT_DATE, -CAST(FLOOR(RAND() * 30) AS INT)))
                + CAST(FLOOR(RAND() * 86400) AS INT)
    ) AS log_time
-- 生成1800条数据（通过子查询提前定义visitor_num，解决引用错误）
FROM (
         -- 子查询1：生成基础渠道和访客数
         SELECT
             channel,
             -- 访客数：基于渠道类型分层模拟
             CAST(CASE
                      WHEN channel = '手淘搜索' THEN RAND() * 900 + 100  -- 搜索渠道流量较大：100-1000
                      WHEN channel = '手淘推荐' THEN RAND() * 700 + 50   -- 推荐渠道：50-750
                      ELSE RAND() * 400 + 10                             -- 其他渠道：10-410
                 END AS INT) AS visitor_num
         FROM (
                  -- 子查询2：生成渠道列表
                  SELECT
                      CASE FLOOR(RAND() * 6)
                          WHEN 0 THEN '手淘搜索'
                          WHEN 1 THEN '手淘推荐'
                          WHEN 2 THEN '直通车'
                          WHEN 3 THEN '聚划算'
                          WHEN 4 THEN '直播'
                          ELSE '短视频'
                          END AS channel
                  FROM (
                           SELECT 1 AS num UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1
                           UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1
                       ) t1
                           CROSS JOIN (
                      SELECT 1 AS num UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1
                      UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1
                  ) t2
                           CROSS JOIN (
                      SELECT 1 AS num UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1
                      UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1
                  ) t3
              ) channel_list
     ) temp; -- 外层引用子查询定义的channel和visitor_num字段


select * from ods_traffic_source_log;
-- 标题关键词日志（支持标题优化）
CREATE EXTERNAL TABLE ods_title_keyword_log (
                                                id STRING COMMENT '记录ID',
                                                product_id STRING COMMENT '商品ID',
                                                title STRING COMMENT '商品标题',
                                                root_word STRING COMMENT '标题词根',
                                                keyword_type STRING COMMENT '关键词类型（搜索词/品类词等）',
                                                drain_num INT COMMENT '引流人数',
                                                conversion_rate DOUBLE COMMENT '转化率',
                                                create_time STRING COMMENT '创建时间'
)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

-- 向标题关键词日志表插入模拟数据，适配商品360看板标题优化需求（词根分析、关键词效果分析）
INSERT INTO TABLE ods_title_keyword_log
SELECT
    -- 生成唯一记录ID
    CONCAT('title_', row_number() OVER ()) AS id,
    -- 商品ID：与商品核心数据关联，范围1000-2000
    CAST(FLOOR(RAND() * 1000) + 1000 AS STRING) AS product_id,
    -- 商品标题：模拟含核心词根的标题（支撑词根拆分分析）
    CONCAT(
            base_title,
            ' ',
            CASE WHEN RAND() > 0.5 THEN '2025新款' ELSE '正品保障' END
    ) AS title,
    -- 标题词根：从标题中提取核心词根（支撑词根引流效果分析）
    root_word,
    -- 关键词类型：模拟搜索词、品类词等类型（支撑关键词效果分析）
    keyword_type,
    -- 引流人数：基于词根类型和商品热度模拟
    CAST(CASE
             WHEN keyword_type = '搜索词' THEN RAND() * 800 + 200  -- 搜索词引流能力强：200-1000
             WHEN keyword_type = '品类词' THEN RAND() * 500 + 100  -- 品类词：100-600
             ELSE RAND() * 300 + 50                               -- 其他类型：50-350
        END AS INT) AS drain_num,
    -- 转化率：与关键词类型相关，搜索词转化率更高
    CAST(CASE
             WHEN keyword_type = '搜索词' THEN RAND() * 0.08 + 0.02  -- 搜索词：2%-10%
             WHEN keyword_type = '长尾词' THEN RAND() * 0.05 + 0.01  -- 长尾词：1%-6%
             ELSE RAND() * 0.03 + 0.005                             -- 其他类型：0.5%-3.5%
        END AS DOUBLE) AS conversion_rate,
    -- 创建时间：近30天内随机时间，与其他表时间范围一致
    FROM_UNIXTIME(
            UNIX_TIMESTAMP(DATE_ADD(CURRENT_DATE, -CAST(FLOOR(RAND() * 30) AS INT)))
                + CAST(FLOOR(RAND() * 86400) AS INT)
    ) AS create_time
-- 生成2000条数据（通过子查询定义基础标题和词根，确保关联性）
FROM (
         -- 子查询1：定义基础标题、词根及关键词类型
         SELECT
             base_title,
             root_word,
             keyword_type
         FROM (
                  -- 子查询2：模拟不同品类的基础标题和核心词根
                  SELECT
                      '夏季连衣裙 显瘦气质' AS base_title,
                      '连衣裙' AS root_word,
                      CASE FLOOR(RAND() * 3)
                          WHEN 0 THEN '搜索词'
                          WHEN 1 THEN '品类词'
                          ELSE '修饰词'
                          END AS keyword_type
                  UNION ALL
                  SELECT
                      '男士运动鞋 透气轻便' AS base_title,
                      '运动鞋' AS root_word,
                      CASE FLOOR(RAND() * 3)
                          WHEN 0 THEN '搜索词'
                          WHEN 1 THEN '品类词'
                          ELSE '修饰词'
                          END AS keyword_type
                  UNION ALL
                  SELECT
                      '无线蓝牙耳机 高音质' AS base_title,
                      '蓝牙耳机' AS root_word,
                      CASE FLOOR(RAND() * 3)
                          WHEN 0 THEN '搜索词'
                          WHEN 1 THEN '品类词'
                          ELSE '长尾词'
                          END AS keyword_type
                  UNION ALL
                  SELECT
                      '儿童纯棉T恤 舒适透气' AS base_title,
                      'T恤' AS root_word,
                      CASE FLOOR(RAND() * 3)
                          WHEN 0 THEN '搜索词'
                          WHEN 1 THEN '品类词'
                          ELSE '长尾词'
                          END AS keyword_type
                  UNION ALL
                  SELECT
                      '全自动洗衣机 大容量' AS base_title,
                      '洗衣机' AS root_word,
                      CASE FLOOR(RAND() * 3)
                          WHEN 0 THEN '搜索词'
                          WHEN 1 THEN '品类词'
                          ELSE '修饰词'
                          END AS keyword_type
              ) title_base
                  -- 交叉连接生成足够数据量
                  CROSS JOIN (
             SELECT 1 AS num UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1
             UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1
         ) t1
                  CROSS JOIN (
             SELECT 1 AS num UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1
             UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1
         ) t2
                  CROSS JOIN (
             SELECT 1 AS num UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1
         ) t3
     ) temp;

select * from ods_title_keyword_log;
-- 内容数据日志（支持内容分析）
CREATE EXTERNAL TABLE ods_content_data_log (
                                               id STRING COMMENT '记录ID',
                                               product_id STRING COMMENT '商品ID',
                                               content_type STRING COMMENT '内容类型（直播/短视频/图文）',
                                               content_id STRING COMMENT '内容ID',
                                               view_num INT COMMENT '浏览量',
                                               conversion_num INT COMMENT '转化数',
                                               create_time STRING COMMENT '创建时间'
)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

-- 向内容数据日志表插入模拟数据，适配商品360看板内容分析需求
INSERT INTO TABLE ods_content_data_log
SELECT
    -- 生成唯一记录ID
    CONCAT('content_', row_number() OVER ()) AS id,
    -- 商品ID：与商品核心数据关联，范围1000-2000
    CAST(FLOOR(RAND() * 1000) + 1000 AS STRING) AS product_id,
    -- 内容类型：模拟直播、短视频、图文三种类型
    content_type,
    -- 内容ID：基于内容类型和随机数生成，确保唯一性
    CONCAT(content_type, '_', CAST(FLOOR(RAND() * 10000) AS STRING)) AS content_id,
    -- 浏览量：基于内容类型分层模拟
    view_num,
    -- 转化数：基于浏览量和内容类型计算
    CAST(CASE
             WHEN content_type = '直播' THEN view_num * (RAND() * 0.06 + 0.04)
             WHEN content_type = '短视频' THEN view_num * (RAND() * 0.04 + 0.02)
             ELSE view_num * (RAND() * 0.03 + 0.01)
        END AS INT) AS conversion_num,
    -- 创建时间：近30天内随机时间
    FROM_UNIXTIME(
            UNIX_TIMESTAMP(DATE_ADD(CURRENT_DATE, -CAST(FLOOR(RAND() * 30) AS INT)))
                + CAST(FLOOR(RAND() * 86400) AS INT)
    ) AS create_time
-- 通过子查询提前定义内容类型和浏览量，避免字段引用错误
FROM (
         SELECT
             content_type,
             -- 按内容类型生成对应浏览量
             CAST(CASE
                      WHEN content_type = '直播' THEN RAND() * 1800 + 200
                      WHEN content_type = '短视频' THEN RAND() * 1400 + 100
                      ELSE RAND() * 900 + 50
                 END AS INT) AS view_num
         FROM (
                  -- 生成内容类型列表
                  SELECT
                      CASE FLOOR(RAND() * 3)
                          WHEN 0 THEN '直播'
                          WHEN 1 THEN '短视频'
                          ELSE '图文'
                          END AS content_type
                  FROM (
                           -- 生成基础数据量（2000条）
                           SELECT 1 FROM (
                                             SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1
                                             UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1
                                         ) t1
                                             CROSS JOIN (
                               SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1
                               UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1
                           ) t2
                                             CROSS JOIN (
                               SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1
                           ) t3
                       ) temp
              ) content_list
     ) data_source;

select * from ods_content_data_log;
-- 用户画像日志（支持客群洞察）
CREATE EXTERNAL TABLE ods_user_portrait_log (
                                                id STRING COMMENT '记录ID',
                                                product_id STRING COMMENT '商品ID',
                                                user_behavior_type STRING COMMENT '用户行为类型（搜索/访问/支付）',
                                                user_tag STRING COMMENT '用户标签（如年龄、性别）',
                                                user_count INT COMMENT '人群数量',
                                                create_time STRING COMMENT '创建时间'
)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

-- 向用户画像日志表插入模拟数据，适配商品360看板客群洞察需求（修正用户行为类型引用错误）
INSERT INTO TABLE ods_user_portrait_log
SELECT
    -- 生成唯一记录ID
    CONCAT('user_portrait_', row_number() OVER ()) AS id,
    -- 商品ID：与商品核心数据关联，范围1000-2000
    CAST(FLOOR(RAND() * 1000) + 1000 AS STRING) AS product_id,
    -- 用户行为类型：引用子查询定义的类型
    user_behavior_type,
    -- 用户标签：模拟年龄+性别组合
    user_tag,
    -- 人群数量：基于行为类型分层模拟
    CAST(CASE
             WHEN user_behavior_type = '搜索' THEN RAND() * 1800 + 200
             WHEN user_behavior_type = '访问' THEN RAND() * 900 + 100
             ELSE RAND() * 400 + 50
        END AS INT) AS user_count,
    -- 创建时间：近30天内随机时间
    FROM_UNIXTIME(
            UNIX_TIMESTAMP(DATE_ADD(CURRENT_DATE, -CAST(FLOOR(RAND() * 30) AS INT)))
                + CAST(FLOOR(RAND() * 86400) AS INT)
    ) AS create_time
-- 通过子查询提前定义用户行为类型和标签，避免引用错误
FROM (
         SELECT
             user_behavior_type,
             -- 生成用户标签
             CONCAT(
                     CASE FLOOR(RAND() * 5)
                         WHEN 0 THEN '年龄:18-25'
                         WHEN 1 THEN '年龄:26-35'
                         WHEN 2 THEN '年龄:36-45'
                         WHEN 3 THEN '年龄:46-55'
                         ELSE '年龄:56+'
                         END,
                     ',',
                     CASE FLOOR(RAND() * 2)
                         WHEN 0 THEN '性别:男'
                         ELSE '性别:女'
                         END
             ) AS user_tag
         FROM (
                  -- 生成用户行为类型列表
                  SELECT
                      CASE FLOOR(RAND() * 3)
                          WHEN 0 THEN '搜索'
                          WHEN 1 THEN '访问'
                          ELSE '支付'
                          END AS user_behavior_type
                  FROM (
                           -- 生成基础数据量
                           SELECT 1 FROM (
                                             SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1
                                             UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1
                                         ) t1
                                             CROSS JOIN (
                               SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1
                               UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1
                           ) t2
                                             CROSS JOIN (
                               SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1
                           ) t3
                       ) temp
              ) behavior_types
     ) data_source;

select * from ods_user_portrait_log;

-- 关联搭配日志（支持关联搭配分析）
CREATE EXTERNAL TABLE ods_association_log (
                                              id STRING COMMENT '记录ID',
                                              product_id STRING COMMENT '单品ID',
                                              related_product_id STRING COMMENT '关联商品ID',
                                              drainage_num INT COMMENT '引流人数',
                                              create_time STRING COMMENT '创建时间'
)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

-- 向关联商品日志表插入模拟数据，适配商品360看板关联搭配需求（修正product_id引用错误）
INSERT INTO TABLE ods_association_log
SELECT
    -- 生成唯一记录ID
    CONCAT('assoc_', row_number() OVER ()) AS id,
    -- 单品ID：引用子查询定义的product_id
    product_id,
    -- 关联商品ID：与单品ID范围不同，避免自关联
    related_product_id,
    -- 引流人数：基于单品热度分层模拟
    CAST(CASE
             WHEN product_id BETWEEN '1500' AND '1800' THEN RAND() * 900 + 100  -- 热销单品引流更强
             ELSE RAND() * 400 + 50                                           -- 普通单品引流较弱
        END AS INT) AS drainage_num,
    -- 创建时间：近30天内随机时间
    FROM_UNIXTIME(
            UNIX_TIMESTAMP(DATE_ADD(CURRENT_DATE, -CAST(FLOOR(RAND() * 30) AS INT)))
                + CAST(FLOOR(RAND() * 86400) AS INT)
    ) AS create_time
-- 通过子查询提前定义单品ID和关联商品ID，解决引用错误
FROM (
         SELECT
             -- 单品ID：1000-2000
             CAST(FLOOR(RAND() * 1000) + 1000 AS STRING) AS product_id,
             -- 关联商品ID：500-999或2000-2499
             CAST(CASE
                      WHEN FLOOR(RAND() * 2) = 0 THEN FLOOR(RAND() * 500) + 500
                      ELSE FLOOR(RAND() * 500) + 2000
                 END AS STRING) AS related_product_id
         FROM (
                  -- 生成基础数据量（1500条）
                  SELECT 1 FROM (
                                    SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1
                                    UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1
                                ) t1
                                    CROSS JOIN (
                      SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1
                      UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1
                  ) t2
                                    CROSS JOIN (
                      SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1
                  ) t3
              ) temp
     ) data_source;

select * from ods_association_log;
-- 评价日志（支持服务体验分析）
CREATE EXTERNAL TABLE ods_evaluation_log (
                                             id STRING COMMENT '记录ID',
                                             product_id STRING COMMENT '商品ID',
                                             sku_id STRING COMMENT 'SKU ID',
                                             user_type STRING COMMENT '用户类型（整体/老买家）',
                                             score INT COMMENT '评分',
                                             evaluation_content STRING COMMENT '评价内容',
                                             eval_time STRING COMMENT '评价时间'
)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';


-- 向评价日志表插入模拟数据，适配商品360看板服务体验中的评价分析需求（修正product_id引用错误）
INSERT INTO TABLE ods_evaluation_log
SELECT
    -- 生成唯一记录ID
    CONCAT('eval_', row_number() OVER ()) AS id,
    -- 商品ID：引用子查询定义的product_id
    product_id,
    -- SKU ID：基于商品ID生成，确保关联性
    CONCAT(product_id, '_', CAST(FLOOR(RAND() * 100) AS STRING)) AS sku_id,
    -- 用户类型：模拟整体用户和老买家
    user_type,
    -- 评分：1-5分，高分占比更高
    score,
    -- 评价内容：基于评分生成正向或负向评价
    CASE
        WHEN score >= 4 THEN CONCAT('商品很好，非常满意，推荐购买！',
                                    CASE FLOOR(RAND() * 3)
                                        WHEN 0 THEN '质量不错'
                                        WHEN 1 THEN '物流很快'
                                        ELSE '性价比高'
                                        END)
        WHEN score = 3 THEN '商品一般，有改进空间，总体还行。'
        ELSE CONCAT('不太满意，',
                    CASE FLOOR(RAND() * 3)
                        WHEN 0 THEN '质量不符合预期'
                        WHEN 1 THEN '物流较慢'
                        ELSE '性价比不高'
                        END)
        END AS evaluation_content,
    -- 评价时间：近90天内随机时间
    FROM_UNIXTIME(
            UNIX_TIMESTAMP(DATE_ADD(CURRENT_DATE, -CAST(FLOOR(RAND() * 90) AS INT)))
                + CAST(FLOOR(RAND() * 86400) AS INT)
    ) AS eval_time
-- 通过子查询提前定义商品ID、用户类型和评分，解决引用错误
FROM (
         SELECT
             -- 商品ID：1000-2000
             CAST(FLOOR(RAND() * 1000) + 1000 AS STRING) AS product_id,
             -- 用户类型：整体/老买家
             CASE FLOOR(RAND() * 2)
                 WHEN 0 THEN '整体'
                 ELSE '老买家'
                 END AS user_type,
             -- 评分：1-5分，3-5分占比80%
             CAST(CASE
                      WHEN RAND() > 0.2 THEN FLOOR(RAND() * 3) + 3
                      ELSE FLOOR(RAND() * 2) + 1
                 END AS INT) AS score
         FROM (
                  -- 生成基础数据量（3000条）
                  SELECT 1 FROM (
                                    SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1
                                    UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1
                                ) t1
                                    CROSS JOIN (
                      SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1
                      UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1
                  ) t2
                                    CROSS JOIN (
                      SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1
                  ) t3
              ) temp
     ) data_source;

select * from ods_evaluation_log;