CREATE DATABASE IF NOT EXISTS rewrite_work_order_03;

USE rewrite_work_order_03;

-- TODO 1. 商品基础信息表（ods_product_info）
DROP TABLE IF EXISTS ods_product_info;
CREATE EXTERNAL TABLE IF NOT EXISTS ods_product_info
(
    product_id    STRING COMMENT '商品ID',
    product_name  STRING COMMENT '商品名称',
    category_id   STRING COMMENT '类目ID',
    category_name STRING COMMENT '类目名称',
    price         DECIMAL(10, 2) COMMENT '商品标价',
    create_time   STRING COMMENT '商品创建时间',
    status        STRING COMMENT '商品状态（上架/下架）'
)
    COMMENT '商品基础信息原始表'
    PARTITIONED BY (dt STRING)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
        WITH SERDEPROPERTIES (
        'field.delim' = '\t',
        'serialization.format' = '\t'
        )
    STORED AS TEXTFILE;

-- 1. 彻底关闭本地模式及相关优化，强制分布式执行
set hive.exec.mode.local.auto=false;
set hive.auto.convert.join=false;  -- 关闭自动转换为本地JOIN
set mapreduce.job.reduces=1;  -- 固定Reduce数量，减少资源调度压力

-- 2. 先清空目标分区（若有旧数据冲突）
ALTER TABLE ods_product_info DROP IF EXISTS PARTITION (dt='2025-01-26');

-- 3. 插入极简模拟数据（最小化计算逻辑，确保执行成功）
INSERT INTO TABLE ods_product_info PARTITION (dt='2025-01-26')
SELECT
    product_id,
    product_name,
    category_id,
    category_name,
    price,
    create_time,
    status
FROM (
         -- 直接生成固定数据，避免复杂计算和JOIN
         SELECT 'product_100001' as product_id, '女装-基础款-黑' as product_name, '101' as category_id, '女装' as category_name, 100.00 as price, '2025-01-10 10:00:00' as create_time, '上架' as status UNION ALL
         SELECT 'product_100002' as product_id, '女装-热销款-白' as product_name, '101' as category_id, '女装' as category_name, 110.00 as price, '2025-01-11 10:00:00' as create_time, '上架' as status UNION ALL
         SELECT 'product_200001' as product_id, '手机-基础款-红' as product_name, '202' as category_id, '手机' as category_name, 2000.00 as price, '2025-01-10 10:00:00' as create_time, '上架' as status UNION ALL
         SELECT 'product_300001' as product_id, '厨具-特惠款-蓝' as product_name, '301' as category_id, '厨具' as category_name, 50.00 as price, '2025-01-10 10:00:00' as create_time, '下架' as status
     ) data_gen;

select * from ods_product_info;
-- TODO 2. SKU 信息表（ods_sku_info）
DROP TABLE IF EXISTS ods_sku_info;
CREATE EXTERNAL TABLE IF NOT EXISTS ods_sku_info
(
    sku_id     STRING COMMENT 'SKU ID',
    product_id STRING COMMENT '关联商品ID',
    sku_name   STRING COMMENT 'SKU名称',
    sku_attr   STRING COMMENT 'SKU属性（如"颜色:红色;尺寸:XL"）',
    sku_price  DECIMAL(10, 2) COMMENT 'SKU售价',
    stock_num  BIGINT COMMENT '库存数量'
)
    COMMENT 'SKU原始信息表'
    PARTITIONED BY (dt STRING)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
        WITH SERDEPROPERTIES (
        'field.delim' = '\t',
        'serialization.format' = '\t'
        )
    STORED AS TEXTFILE;

-- 向 ods_sku_info 表插入模拟数据（以2025-01-26为例，与商品表关联）
INSERT INTO TABLE ods_sku_info PARTITION (dt='2025-01-26')
SELECT
    sku_id,
    product_id,
    sku_name,
    sku_attr,
    sku_price,
    stock_num
FROM (
         SELECT
             -- SKU ID：商品ID+随机后缀（确保与商品关联）
             concat(p.product_id, '_sku_', lpad(cast(t1.pos as int), 2, '0')) as sku_id,
             -- 关联商品ID（取自商品表，确保存在）
             p.product_id,
             -- SKU名称：商品名称+属性（如"女装-基础款-黑_红色-XL"）
             concat(p.product_name, '_',
                    case when t1.pos % 3 = 0 then '红色' when t1.pos % 3 = 1 then '蓝色' else '黑色' end, '-',
                    case when t1.pos % 2 = 0 then 'XL' else 'L' end) as sku_name,
             -- SKU属性：格式"颜色:值;尺寸:值"
             concat(
                     '颜色:', case when t1.pos % 3 = 0 then '红色' when t1.pos % 3 = 1 then '蓝色' else '黑色' end, ';',
                     '尺寸:', case when t1.pos % 2 = 0 then 'XL' else 'L' end
             ) as sku_attr,
             -- SKU售价：在商品标价基础上浮动±10%
             round(p.price * (1 + (rand() * 0.2 - 0.1)), 2) as sku_price,
             -- 库存数量：50-200之间随机
             50 + floor(rand() * 150) as stock_num
         FROM (
                  -- 从商品表获取已存在的商品（确保关联有效性）
                  SELECT product_id, product_name, price
                  FROM ods_product_info
                  WHERE dt='2025-01-26'  -- 与商品表同日期，保证数据关联
                    AND status='上架'  -- 只给上架商品生成SKU
                  LIMIT 100  -- 限制商品数量，避免数据过多
              ) p
                  -- 为每个商品生成3个SKU（通过t1.pos控制）
                  JOIN (
             SELECT 1 AS pos UNION ALL SELECT 2 UNION ALL SELECT 3
         ) t1 ON 1=1
     ) data_gen;

select * from ods_sku_info;
-- TODO 3. 用户行为表（ods_user_behavior）
DROP TABLE IF EXISTS ods_user_behavior;
CREATE EXTERNAL TABLE IF NOT EXISTS ods_user_behavior
(
    user_id       STRING COMMENT '用户ID',
    product_id    STRING COMMENT '商品ID',
    sku_id        STRING COMMENT 'SKU ID',
    behavior_type STRING COMMENT '行为类型（浏览/加购/支付/搜索）',
    behavior_time STRING COMMENT '行为时间（yyyy-MM-dd HH:mm:ss）',
    channel       STRING COMMENT '行为来源渠道（搜索/直播/短视频等）'
)
    COMMENT '用户行为原始表'
    PARTITIONED BY (dt STRING)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
        WITH SERDEPROPERTIES (
        'field.delim' = '\t',
        'serialization.format' = '\t'
        )
    STORED AS TEXTFILE;

-- 向 ods_user_behavior 表插入模拟数据（dt='2025-01-26'）
INSERT INTO TABLE ods_user_behavior PARTITION (dt='2025-01-26')
SELECT
    user_id,
    product_id,
    sku_id,
    behavior_type,
    behavior_time,
    channel
FROM (
         SELECT
             -- 用户ID：固定前缀+随机数（确保非空）
             concat('user_', lpad(cast(floor(rand() * 900000 + 100000) as int), 6, '0')) as user_id,
             -- 商品ID：从商品表随机获取（确保存在且非空）
             p.product_id,
             -- SKU ID：从SKU表随机获取（与商品ID关联，确保非空）
             s.sku_id,
             -- 行为类型：浏览(50%)、加购(20%)、支付(20%)、搜索(10%)
             case
                 when rand() <= 0.5 then '浏览'
                 when rand() <= 0.7 then '加购'
                 when rand() <= 0.9 then '支付'
                 else '搜索'
                 end as behavior_type,
             -- 行为时间：2025-01-26当天的随机时间（非空）
             concat(
                     '2025-01-26 ',
                     lpad(cast(8 + floor(rand() * 12) as int), 2, '0'), ':',  -- 8-20点
                     lpad(cast(floor(rand() * 60) as int), 2, '0'), ':',
                     lpad(cast(floor(rand() * 60) as int), 2, '0')
             ) as behavior_time,
             -- 渠道：搜索(30%)、直播(25%)、短视频(25%)、推荐(20%)
             case
                 when rand() <= 0.3 then '搜索'
                 when rand() <= 0.55 then '直播'
                 when rand() <= 0.8 then '短视频'
                 else '推荐'
                 end as channel
         FROM (
                  -- 从商品表获取上架商品（确保product_id有效）
                  SELECT product_id FROM ods_product_info
                  WHERE dt='2025-01-26' AND status='上架'
              ) p
                  -- 关联SKU表（确保sku_id与product_id匹配）
                  JOIN (
             SELECT sku_id, product_id FROM ods_sku_info
             WHERE dt='2025-01-26'
         ) s ON p.product_id = s.product_id
             -- 扩展数据量：通过多重JOIN生成约 商品数×SKU数×50 条数据（确保>100）
                  JOIN (
             SELECT 1 AS pos UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5
             UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9 UNION ALL SELECT 10
         ) t1 ON 1=1
                  JOIN (
             SELECT 1 AS pos UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5
         ) t2 ON 1=1
     ) data_gen;

select * from ods_user_behavior;
-- TODO 4. 内容数据表（ods_content_info）
DROP TABLE IF EXISTS ods_content_info;
CREATE EXTERNAL TABLE IF NOT EXISTS ods_content_info
(
    content_id     STRING COMMENT '内容ID',
    product_id     STRING COMMENT '关联商品ID',
    content_type   STRING COMMENT '内容类型（直播/短视频/图文）',
    view_count     BIGINT COMMENT '播放量/阅读量',
    interact_count BIGINT COMMENT '互动量（评论/点赞）',
    publish_time   STRING COMMENT '发布时间',
    author_id      STRING COMMENT '内容创作者ID'
)
    COMMENT '内容数据原始表'
    PARTITIONED BY (dt STRING)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
        WITH SERDEPROPERTIES (
        'field.delim' = '\t',
        'serialization.format' = '\t'
        )
    STORED AS TEXTFILE;

-- Insert data into ods_content_info table by joining with related ODS tables
INSERT INTO TABLE ods_content_info PARTITION (dt='2025-01-26')
SELECT
    concat(content_type, '_', lpad(cast(floor(rand() * 90000 + 10000) as int), 5, '0')) as content_id,
    p.product_id,
    content_type,
    view_count,
    interact_count,
    publish_time,
    author_id
FROM (
         SELECT
             p.product_id,
             CASE
                 WHEN rand() <= 0.3 THEN '直播'
                 WHEN rand() <= 0.8 THEN '短视频'
                 ELSE '图文'
                 END as content_type,
             CASE
                 WHEN (CASE WHEN rand() <= 0.3 THEN '直播' WHEN rand() <= 0.8 THEN '短视频' ELSE '图文' END) = '直播'
                     THEN 10000 + floor(rand() * 90000)  -- 1-10万
                 WHEN (CASE WHEN rand() <= 0.3 THEN '直播' WHEN rand() <= 0.8 THEN '短视频' ELSE '图文' END) = '短视频'
                     THEN 5000 + floor(rand() * 45000)   -- 0.5-5万
                 ELSE 1000 + floor(rand() * 9000)        -- 0.1-1万（图文）
                 END as view_count,
             CAST(
                     (CASE
                          WHEN (CASE WHEN rand() <= 0.3 THEN '直播' WHEN rand() <= 0.8 THEN '短视频' ELSE '图文' END) = '直播'
                              THEN 10000 + floor(rand() * 90000)
                          WHEN (CASE WHEN rand() <= 0.3 THEN '直播' WHEN rand() <= 0.8 THEN '短视频' ELSE '图文' END) = '短视频'
                              THEN 5000 + floor(rand() * 45000)
                          ELSE 1000 + floor(rand() * 9000)
                         END) * (0.05 + rand() * 0.1) as bigint
             ) as interact_count,
             concat(
                     '2025-01-',
                     lpad(cast(20 + floor(rand() * 7) as int), 2, '0'), ' ',
                     lpad(cast(10 + floor(rand() * 12) as int), 2, '0'), ':',
                     lpad(cast(floor(rand() * 60) as int), 2, '0'), ':',
                     lpad(cast(floor(rand() * 60) as int), 2, '0')
             ) as publish_time,
             concat('author_', lpad(cast(floor(rand() * 9000 + 1000) as int), 4, '0')) as author_id
         FROM (
                  -- Get products that are actually being interacted with (from user behavior)
                  SELECT DISTINCT ub.product_id
                  FROM ods_user_behavior ub
                  WHERE ub.dt='2025-01-26'
                    AND ub.behavior_type IN ('浏览', '加购', '支付')

                  UNION ALL

                  -- Also include some products that might have content but no interactions yet
                  SELECT p.product_id
                  FROM ods_product_info p
                  WHERE p.dt='2025-01-26'
                    AND p.status='上架'
                  ORDER BY rand()
                  LIMIT 5
              ) p
                  -- Expand to generate multiple content items per product
                  JOIN (
             SELECT 1 AS pos UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL
             SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6
         ) t1 ON 1=1
     ) data_gen;


-- 向 ods_content_info 表插入模拟数据（dt='2025-01-26'）
INSERT INTO TABLE ods_content_info PARTITION (dt='2025-01-26')
SELECT
    content_id,
    product_id,
    content_type,
    view_count,
    interact_count,
    publish_time,
    author_id
FROM (
         -- 第二层：生成 content_id（依赖上一层的 content_type）
         SELECT
             concat(content_type, '_', lpad(cast(floor(rand() * 90000 + 10000) as int), 5, '0')) as content_id,
             product_id,
             content_type,
             view_count,
             interact_count,
             publish_time,
             author_id
         FROM (
                  -- 第一层：先定义 content_type 等基础字段
                  SELECT
                      p.product_id,
                      -- 内容类型：直播(30%)、短视频(50%)、图文(20%)
                      case
                          when rand() <= 0.3 then '直播'
                          when rand() <= 0.8 then '短视频'
                          else '图文'
                          end as content_type,
                      -- 播放量/阅读量：根据内容类型区分
                      case
                          when (case when rand() <= 0.3 then '直播' when rand() <= 0.8 then '短视频' else '图文' end) = '直播'
                              then 10000 + floor(rand() * 90000)  -- 1-10万
                          when (case when rand() <= 0.3 then '直播' when rand() <= 0.8 then '短视频' else '图文' end) = '短视频'
                              then 5000 + floor(rand() * 45000)   -- 0.5-5万
                          else 1000 + floor(rand() * 9000)  -- 0.1-1万（图文）
                          end as view_count,
                      -- 互动量：播放量的5%-15%
                      cast(
                              (case
                                   when (case when rand() <= 0.3 then '直播' when rand() <= 0.8 then '短视频' else '图文' end) = '直播'
                                       then 10000 + floor(rand() * 90000)
                                   when (case when rand() <= 0.3 then '直播' when rand() <= 0.8 then '短视频' else '图文' end) = '短视频'
                                       then 5000 + floor(rand() * 45000)
                                   else 1000 + floor(rand() * 9000)
                                  end) * (0.05 + rand() * 0.1)
                          as bigint
                      ) as interact_count,
                      -- 发布时间：近7天内的随机时间
                      concat(
                              '2025-01-', lpad(cast(20 + floor(rand() * 7) as int), 2, '0'), ' ',
                              lpad(cast(10 + floor(rand() * 12) as int), 2, '0'), ':',
                              lpad(cast(floor(rand() * 60) as int), 2, '0'), ':',
                              lpad(cast(floor(rand() * 60) as int), 2, '0')
                      ) as publish_time,
                      -- 创作者ID
                      concat('author_', lpad(cast(floor(rand() * 9000 + 1000) as int), 4, '0')) as author_id
                  FROM (
                           -- 从商品表获取上架商品
                           SELECT product_id FROM ods_product_info
                           WHERE dt='2025-01-26' AND status='上架'
                       ) p
                           -- 扩展数据量（确保>100条）
                           JOIN (
                      SELECT 1 AS pos UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5
                      UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9 UNION ALL SELECT 10
                  ) t1 ON 1=1
                           JOIN (
                      SELECT 1 AS pos UNION ALL SELECT 2
                  ) t2 ON 1=1
              ) base_fields
     ) add_content_id;

select * from ods_content_info;
-- TODO 5. 评价表（ods_review_info）
DROP TABLE IF EXISTS ods_review_info;
CREATE EXTERNAL TABLE IF NOT EXISTS ods_review_info
(
    review_id   STRING COMMENT '评价ID',
    product_id  STRING COMMENT '商品ID',
    sku_id      STRING COMMENT 'SKU ID',
    user_id     STRING COMMENT '用户ID',
    score       INT COMMENT '评分（1-5分）',
    content     STRING COMMENT '评价内容',
    create_time STRING COMMENT '评价时间',
    user_type   STRING COMMENT '用户类型（老买家/新买家）'
)
    COMMENT '商品评价原始表'
    PARTITIONED BY (dt STRING)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
        WITH SERDEPROPERTIES (
        'field.delim' = '\t',
        'serialization.format' = '\t'
        )
    STORED AS TEXTFILE;

-- 向 ods_review_info 表插入模拟数据（dt='2025-01-26'）
INSERT INTO TABLE ods_review_info PARTITION (dt='2025-01-26')
SELECT
    review_id,
    product_id,
    sku_id,
    user_id,
    score,
    content,
    create_time,
    user_type
FROM (
         -- 生成评价ID（依赖基础字段）
         SELECT
             concat('review_', product_id, '_', user_id, '_', lpad(cast(floor(rand() * 100) as int), 2, '0')) as review_id,
             product_id,
             sku_id,
             user_id,
             score,
             content,
             create_time,
             user_type
         FROM (
                  -- 基础评价字段（先通过子查询定义score，再引用）
                  SELECT
                      p.product_id,
                      s.sku_id,
                      u.user_id,
                      score,  -- 引用子查询生成的评分
                      -- 评价内容：基于提前定义的score生成
                      case
                          when score = 5 then '非常满意，质量很好，推荐购买！'
                          when score = 4 then '不错的商品，符合预期，好评。'
                          when score = 3 then '一般般，没有特别惊喜，也不算差。'
                          when score = 2 then '有点失望，不太符合描述，不太推荐。'
                          else '质量很差，不建议购买，体验不好。'
                          end as content,
                      -- 评价时间：支付后1-3天
                      concat(
                              substr(u.behavior_time, 1, 10),
                              ' ', lpad(cast(14 + floor(rand() * 6) as int), 2, '0'), ':',  -- 14-20点
                              lpad(cast(floor(rand() * 60) as int), 2, '0'), ':',
                              lpad(cast(floor(rand() * 60) as int), 2, '0')
                      ) as create_time,
                      -- 用户类型：老买家(60%)/新买家(40%)
                      case when rand() <= 0.6 then '老买家' else '新买家' end as user_type
                  FROM (
                           -- 从用户行为表获取有支付行为的用户
                           SELECT distinct user_id, product_id, sku_id, behavior_time
                           FROM ods_user_behavior
                           WHERE dt='2025-01-26' AND behavior_type='支付'
                       ) u
                           -- 关联商品表（确保商品存在）
                           JOIN (
                      SELECT product_id FROM ods_product_info
                      WHERE dt='2025-01-26' AND status='上架'
                  ) p ON u.product_id = p.product_id
                      -- 关联SKU表（确保SKU与商品匹配）
                           JOIN (
                      SELECT sku_id, product_id FROM ods_sku_info
                      WHERE dt='2025-01-26'
                  ) s ON u.sku_id = s.sku_id AND u.product_id = s.product_id
                      -- 扩展数据量
                           JOIN (
                      SELECT 1 AS pos UNION ALL SELECT 2
                  ) t1 ON 1=1
                      -- 关键：提前通过子查询定义评分（避免超前引用）
                           JOIN (
                      SELECT
                          -- 评分逻辑：1-5分（70%为5分，20%为4分，其余低评分）
                          case
                              when rand() <= 0.7 then 5
                              when rand() <= 0.9 then 4
                              when rand() <= 0.95 then 3
                              when rand() <= 0.98 then 2
                              else 1
                              end as score
                  ) score_gen ON 1=1
              ) base_review
     ) add_review_id;

select * from ods_review_info;
