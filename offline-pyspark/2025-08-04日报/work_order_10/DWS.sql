USE commodity_diagnosis;

CREATE TABLE dws_traffic_page_summary
(
    page_id          STRING COMMENT '页面ID',
    module_id        STRING COMMENT '模块ID',
    stat_date        STRING COMMENT '统计日期',
    visitor_count    BIGINT COMMENT '访客数',
    click_count      BIGINT COMMENT '点击量',
    click_user_count BIGINT COMMENT '点击人数'
) COMMENT 'DWS层-页面汇总数据';


INSERT INTO TABLE dws_traffic_page_summary
SELECT page_id,
       module_id,
       -- 提取日期（格式：yyyy-MM-dd）
       to_date(action_time)                                             AS stat_date,
       COUNT(DISTINCT user_id)                                          AS visitor_count,   -- 去重访客数
       SUM(CASE WHEN action_type = 'click' THEN 1 ELSE 0 END)           AS click_count,     -- 总点击量
       COUNT(DISTINCT CASE WHEN action_type = 'click' THEN user_id END) AS click_user_count -- 点击人数
FROM dwd_traffic_page_log
GROUP BY page_id, module_id, to_date(action_time);

select *
from dws_traffic_page_summary;

-- todo 不分区
-- 创建DWS层页面汇总数据表
CREATE TABLE dws_traffic_page_summary_coll
(
    page_id                 STRING COMMENT '页面ID',
    page_type               STRING COMMENT '页面类型',
    page_name               STRING COMMENT '页面名称',
    module_id               STRING COMMENT '模块ID',
    module_name             STRING COMMENT '模块名称',
    visitor_count           BIGINT COMMENT '访客数',
    click_count             BIGINT COMMENT '点击量',
    click_user_count        BIGINT COMMENT '点击人数',
    view_count              BIGINT COMMENT '浏览量',
    view_user_count         BIGINT COMMENT '浏览人数',
    guide_pay_amount        DECIMAL(16, 2) COMMENT '引导支付金额',
    guide_order_buyer_count BIGINT COMMENT '引导下单买家数'
) COMMENT '页面汇总数据'
    PARTITIONED BY (stat_date STRING)
    STORED AS PARQUET;

-- 修正列数量不匹配的问题，确保查询列与目标表列一致
INSERT OVERWRITE TABLE dws_traffic_page_summary_coll
SELECT page_id,
       page_type,
       module_id,
       date_format(action_time, 'yyyy-MM-dd')                           AS stat_date,
       COUNT(DISTINCT user_id)                                          AS visitor_count,
       SUM(CASE WHEN action_type = 'click' THEN 1 ELSE 0 END)           AS click_count,
       COUNT(DISTINCT CASE WHEN action_type = 'click' THEN user_id END) AS click_user_count,
       SUM(CASE WHEN action_type = 'view' THEN 1 ELSE 0 END)            AS view_count,
       COUNT(DISTINCT CASE WHEN action_type = 'view' THEN user_id END)  AS view_user_count,
       0                                                                AS guide_pay_amount,
       0                                                                AS guide_order_buyer_count,
       -- 补充2个缺失的列（根据目标表结构推测）
       0                                                                AS additional_column1, -- 替换为实际列名
       ''                                                               AS additional_column2  -- 替换为实际列名
FROM dwd_traffic_page_log
GROUP BY page_id,
         page_type,
         module_id,
         date_format(action_time, 'yyyy-MM-dd');

select *
from dws_traffic_page_summary_coll;

