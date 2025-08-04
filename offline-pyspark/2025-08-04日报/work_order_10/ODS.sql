CREATE DATABASE IF NOT EXISTS Commodity_diagnosis;

USE Commodity_diagnosis;

CREATE TABLE ods_traffic_raw_log
(
    i           INT COMMENT '序号',
    x           STRING COMMENT '占位字段',
    user_id     STRING COMMENT '用户ID',
    page_id     STRING COMMENT '页面ID',
    module_id   STRING COMMENT '模块ID',
    action_time STRING COMMENT '行为时间',
    action_type STRING COMMENT '行为类型：click, view'
) COMMENT 'ODS层-页面访问原始日志表';

INSERT INTO TABLE ods_traffic_raw_log
SELECT i,
       x,
       -- 生成用户ID
       concat('user_', lpad(i, 3, '0'))                          AS user_id,
       -- 生成页面ID（避免直接引用page_type，提前计算）
       CASE
           WHEN rand() < 0.3 THEN concat('home_', lpad(floor(rand() * 100), 3, '0'))
           WHEN rand() < 0.5 THEN concat('custom_', lpad(floor(rand() * 100), 3, '0'))
           ELSE concat('detail_', lpad(floor(rand() * 100), 3, '0'))
           END                                                   AS page_id,
       -- 生成模块ID
       concat('module_', lpad(floor(rand() * 20), 2, '0'))       AS module_id,
       -- 生成行为时间（近30天）
       date_sub(current_date(), cast(floor(rand() * 30) AS INT)) AS action_time,
       -- 生成行为类型（click/view）
       CASE WHEN rand() < 0.4 THEN 'click' ELSE 'view' END       AS action_type
FROM (SELECT posexplode(split(space(999), '')) AS (i, x)) t;

select *
from ods_traffic_raw_log;