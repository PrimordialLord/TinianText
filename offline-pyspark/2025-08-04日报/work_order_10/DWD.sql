USE commodity_diagnosis;

CREATE TABLE dwd_traffic_page_log
(
    user_id     STRING COMMENT '用户ID',
    page_id     STRING COMMENT '页面ID',
    page_type   STRING COMMENT '页面类型：home, custom, detail',
    module_id   STRING COMMENT '模块ID',
    action_time STRING COMMENT '行为时间',
    action_type STRING COMMENT '行为类型：click, view'
) COMMENT 'DWD层-页面行为明细数据';


INSERT INTO TABLE dwd_traffic_page_log
SELECT user_id,
       page_id,
       -- 根据page_id提取页面类型
       CASE
           WHEN page_id LIKE 'home_%' THEN 'home'
           WHEN page_id LIKE 'custom_%' THEN 'custom'
           WHEN page_id LIKE 'detail_%' THEN 'detail'
           ELSE 'unknown'
           END AS page_type,
       module_id,
       action_time,
       action_type
FROM ods_traffic_raw_log;

select *
from dwd_traffic_page_log;


-- 不分区
-- 创建DWD层页面行为明细表
CREATE TABLE dwd_traffic_page_log_coll
(
    user_id        STRING COMMENT '用户ID',
    page_id        STRING COMMENT '页面ID',
    page_type      STRING COMMENT '页面类型',
    page_name      STRING COMMENT '页面名称',
    module_id      STRING COMMENT '模块ID',
    module_name    STRING COMMENT '模块名称',
    action_time    STRING COMMENT '行为时间',
    action_type    STRING COMMENT '行为类型(click/view)',
    guide_pay      DECIMAL(16, 2) COMMENT '引导支付金额',
    guide_buyer_id STRING COMMENT '引导下单买家ID'
) COMMENT '页面行为明细数据'
    PARTITIONED BY (stat_date STRING)
    STORED AS PARQUET;

-- 插入1000条不分区的模拟数据（修正列引用错误）
INSERT INTO TABLE dwd_traffic_page_log_coll
SELECT user_id,
       page_id,
       -- 页面类型（根据page_id推断，现在可以正确引用子查询中的page_id）
       CASE
           WHEN page_id LIKE 'home%' THEN 'home'
           WHEN page_id LIKE 'product%' THEN 'product'
           WHEN page_id LIKE 'order%' THEN 'order'
           ELSE 'other'
           END AS page_type,
       -- 页面名称
       CASE
           WHEN page_id LIKE 'home%' THEN '首页'
           WHEN page_id LIKE 'product%' THEN '商品详情页'
           WHEN page_id LIKE 'order%' THEN '订单页'
           ELSE '其他页面'
           END AS page_name,
       module_id,
       module_name,
       action_time,
       action_type,
       guide_pay,
       guide_buyer_id,
       ''      AS stat_date
FROM (
         -- 子查询中生成所有基础字段，便于后续引用
         SELECT
             -- 生成随机用户ID（user_1到user_100）
             CONCAT('user_', CAST(FLOOR(RAND() * 100) + 1 AS INT))                                            AS user_id,
             -- 生成随机页面ID（按类型区分）
             CASE
                 WHEN RAND() < 0.3 THEN CONCAT('home_', CAST(FLOOR(RAND() * 5) + 1 AS INT))
                 WHEN RAND() < 0.7 THEN CONCAT('product_', CAST(FLOOR(RAND() * 20) + 1 AS INT))
                 ELSE CONCAT('order_', CAST(FLOOR(RAND() * 10) + 1 AS INT))
                 END                                                                                          AS page_id,
             -- 模块ID（module_1到module_8）
             CONCAT('module_', CAST(FLOOR(RAND() * 8) + 1 AS INT))                                            AS module_id,
             -- 模块名称
             CASE CAST(FLOOR(RAND() * 8) + 1 AS INT)
                 WHEN 1 THEN '顶部导航'
                 WHEN 2 THEN '轮播广告'
                 WHEN 3 THEN '商品列表'
                 WHEN 4 THEN '优惠活动'
                 WHEN 5 THEN '用户评论'
                 WHEN 6 THEN '相关推荐'
                 WHEN 7 THEN '底部导航'
                 ELSE '其他模块'
                 END                                                                                          AS module_name,
             -- 行为时间（最近30天内的随机时间）
             DATE_FORMAT(
                     DATE_ADD(CURRENT_TIMESTAMP(), -CAST(FLOOR(RAND() * 30 * 86400) AS INT)),
                     'yyyy-MM-dd HH:mm:ss'
             )                                                                                                AS action_time,
             -- 行为类型（70%浏览，30%点击）
             CASE WHEN RAND() < 0.3 THEN 'click' ELSE 'view' END                                              AS action_type,
             -- 引导支付金额（10%概率有值，0-2000随机）
             CASE
                 WHEN RAND() < 0.1 THEN CAST(RAND() * 2000 AS DECIMAL(16, 2))
                 ELSE 0 END                                                                                   AS guide_pay,
             -- 引导下单买家ID（只有支付时才有值）
             CASE
                 WHEN RAND() < 0.1 THEN CONCAT('buyer_', CAST(FLOOR(RAND() * 100) + 1 AS INT))
                 ELSE NULL END                                                                                AS guide_buyer_id
         FROM
             -- 通过交叉连接生成1000条数据
             (SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5) t1
                 CROSS JOIN (SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5) t2
                 CROSS JOIN (SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5) t3
                 CROSS JOIN (SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4) t4
                 CROSS JOIN (SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4) t5
         LIMIT 1000) t;

select *
from dwd_traffic_page_log_coll;



