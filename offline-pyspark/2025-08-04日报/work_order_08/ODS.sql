CREATE DATABASE IF NOT EXISTS work_order_08;

USE work_order_08;

-- 活动原始信息表
DROP TABLE IF EXISTS ods_activity_info;
CREATE TABLE IF NOT EXISTS ods_activity_info
(
    activity_id    BIGINT COMMENT '活动编号',
    activity_name  STRING COMMENT '活动名称（不超过10字）',
    activity_level STRING COMMENT '活动级别：商品级/SKU级',
    activity_type  STRING COMMENT '优惠类型：固定优惠/自定义优惠',
    start_time     STRING COMMENT '活动开始时间',
    end_time       STRING COMMENT '活动结束时间',
    max_discount   DECIMAL(10, 2) COMMENT '最大优惠金额（自定义优惠时使用）',
    status         STRING COMMENT '活动状态：进行中/已结束/未开始/已暂停',
    create_time    STRING COMMENT '创建时间',
    update_time    STRING COMMENT '更新时间',
    raw_data       STRING COMMENT '原始数据JSON串'
) COMMENT '活动原始信息表'
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE;

-- 向ods_activity_info表插入1000条模拟数据（适配低版本Hive）
INSERT OVERWRITE TABLE ods_activity_info
SELECT id                                                         AS activity_id,
       -- 生成不超过10字的活动名称（符合文档中活动名称长度限制）
       CASE
           WHEN id % 8 = 0 THEN '新品特惠'
           WHEN id % 8 = 1 THEN '老客专享'
           WHEN id % 8 = 2 THEN '限时折扣'
           WHEN id % 8 = 3 THEN '满减活动'
           WHEN id % 8 = 4 THEN '会员回馈'
           WHEN id % 8 = 5 THEN '清仓处理'
           WHEN id % 8 = 6 THEN '周末特卖'
           ELSE '节日优惠'
           END                                                    AS activity_name,
       -- 随机生成活动级别（商品级/SKU级，与文档中活动级别分类一致）
       CASE WHEN id % 2 = 0 THEN '商品级' ELSE 'SKU级' END        AS activity_level,
       -- 随机生成优惠类型（固定优惠/自定义优惠，参考文档中优惠类型划分）
       CASE WHEN id % 3 = 0 THEN '固定优惠' ELSE '自定义优惠' END AS activity_type,
       -- 生成活动开始时间（2025年1月-3月）
       date_add('2025-01-01', cast(rand() * 60 AS INT))           AS start_time,
       -- 活动结束时间（开始时间后2-120天，符合文档中活动时间范围规则{insert\_element\_0\_}）
       date_add(
               date_add('2025-01-01', cast(rand() * 60 AS INT)),
               cast(rand() * 119 + 2 AS INT)
       )                                                          AS end_time,
       -- 自定义优惠时生成最大优惠金额（不超过5000元，遵循文档中金额上限要求{insert\_element\_1\_}）
       CASE
           WHEN id % 3 != 0 THEN cast(rand() * 5000 AS DECIMAL(10, 2))
           ELSE NULL
           END                                                    AS max_discount,
       -- 生成活动状态（进行中/已结束/未开始/已暂停，覆盖文档中活动状态类型）
       CASE
           WHEN date_add('2025-01-01', cast(rand() * 60 AS INT)) > current_date THEN '未开始'
           WHEN date_add(
                        date_add('2025-01-01', cast(rand() * 60 AS INT)),
                        cast(rand() * 119 + 2 AS INT)
                ) < current_date THEN '已结束'
           WHEN id % 5 = 0 THEN '已暂停'
           ELSE '进行中'
           END                                                    AS status,
       -- 生成创建时间（活动开始前）
       date_add(
               date_add('2025-01-01', cast(rand() * 60 AS INT)),
               -cast(rand() * 30 + 1 AS INT)
       )                                                          AS create_time,
       -- 生成更新时间（创建时间后）
       date_add(
               date_add(
                       date_add('2025-01-01', cast(rand() * 60 AS INT)),
                       -cast(rand() * 30 + 1 AS INT)
               ),
               cast(rand() * 10 AS INT)
       )                                                          AS update_time,
       -- 生成原始数据JSON串
       concat(
               '{"activity_id":', id,
               ',"activity_name":"', CASE
                                         WHEN id % 8 = 0 THEN '新品特惠'
                                         WHEN id % 8 = 1 THEN '老客专享'
                                         WHEN id % 8 = 2 THEN '限时折扣'
                                         WHEN id % 8 = 3 THEN '满减活动'
                                         WHEN id % 8 = 4 THEN '会员回馈'
                                         WHEN id % 8 = 5 THEN '清仓处理'
                                         WHEN id % 8 = 6 THEN '周末特卖'
                                         ELSE '节日优惠'
                   END,
               '","activity_level":"', CASE WHEN id % 2 = 0 THEN '商品级' ELSE 'SKU级' END,
               '","activity_type":"', CASE WHEN id % 3 = 0 THEN '固定优惠' ELSE '自定义优惠' END,
               '"}'
       )                                                          AS raw_data
-- 直接通过子查询生成1-1000的自增ID
FROM (SELECT pos + 1 AS id
      FROM (SELECT posexplode(split(space(999), '')) AS (pos, val)) t) temp_activity_data;

select *
from ods_activity_info;


-- 商品活动关联原始表
DROP TABLE IF EXISTS ods_product_activity_rel;
CREATE TABLE IF NOT EXISTS ods_product_activity_rel
(
    id              BIGINT COMMENT '关联记录ID',
    activity_id     BIGINT COMMENT '活动编号',
    product_id      BIGINT COMMENT '商品ID',
    sku_id          BIGINT COMMENT 'SKU ID（商品级活动可为NULL）',
    discount_amount DECIMAL(10, 2) COMMENT '优惠金额',
    limit_purchase  INT COMMENT '每人限购次数',
    is_published    TINYINT COMMENT '是否发布：0未发布/1已发布',
    add_time        STRING COMMENT '添加时间',
    remove_time     STRING COMMENT '移出时间（未移出可为NULL）',
    raw_data        STRING COMMENT '原始数据JSON串'
) COMMENT '商品活动关联原始表'
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE;

-- 向ods_product_activity_rel表插入1000条模拟数据
INSERT OVERWRITE TABLE ods_product_activity_rel
SELECT
    -- 生成唯一关联记录ID
    row_number() OVER ()                      AS id,
    -- 活动ID（关联1-100的活动，符合每个活动最多500个商品的规则）
    cast(rand() * 100 + 1 AS BIGINT)          AS activity_id,
    -- 商品ID（1000-2000范围内随机生成）
    cast(rand() * 1000 + 1000 AS BIGINT)      AS product_id,
    -- SKU ID（商品级活动为NULL，SKU级活动生成10000-15000范围的ID）
    CASE
        WHEN cast(rand() * 2 AS INT) = 0 THEN NULL -- 商品级活动
        ELSE cast(rand() * 5000 + 10000 AS BIGINT) -- SKU级活动
        END                                   AS sku_id,
    -- 优惠金额（1-5000的整数，符合文档中金额为整数且不超过5000元的规则）
    cast(rand() * 5000 + 1 AS DECIMAL(10, 2)) AS discount_amount,
    -- 限购次数（1-5次，默认1次可修改）
    cast(rand() * 4 + 1 AS INT)               AS limit_purchase,
    -- 是否发布（0未发布/1已发布，随机生成）
    cast(rand() * 2 AS TINYINT)               AS is_published,
    -- 添加时间（活动开始时间后1-30天）
    date_add(
            date_add('2025-01-01', cast(rand() * 60 AS INT)),
            cast(rand() * 30 + 1 AS INT)
    )                                         AS add_time,
    -- 移出时间（已发布商品不可移出，未发布商品可能有移出时间）
    CASE
        WHEN cast(rand() * 2 AS TINYINT) = 0 THEN NULL -- 未移出
        WHEN cast(rand() * 2 AS TINYINT) = 1 AND cast(rand() * 2 AS TINYINT) = 0
            THEN date_add(current_date, -cast(rand() * 10 + 1 AS INT)) -- 未发布商品的移出时间
        ELSE NULL -- 已发布商品不可移出
        END                                   AS remove_time,
    -- 原始数据JSON串
    concat(
            '{"id":', row_number() OVER (),
            ',"activity_id":', cast(rand() * 100 + 1 AS BIGINT),
            ',"product_id":', cast(rand() * 1000 + 1000 AS BIGINT),
            ',"sku_id":', CASE
                              WHEN cast(rand() * 2 AS INT) = 0 THEN 'null'
                              ELSE cast(rand() * 5000 + 10000 AS BIGINT)
                END,
            ',"discount_amount":', cast(rand() * 5000 + 1 AS INT),
            ',"limit_purchase":', cast(rand() * 4 + 1 AS INT),
            ',"is_published":', cast(rand() * 2 AS TINYINT),
            '}'
    )                                         AS raw_data
-- 生成1000条数据
FROM (SELECT pos
      FROM (SELECT posexplode(split(space(999), '')) AS (pos, val)) t) temp;

select *
from ods_product_activity_rel;





-- 客服发送优惠原始明细表
DROP TABLE IF EXISTS ods_customer_service_send_detail;
CREATE TABLE IF NOT EXISTS ods_customer_service_send_detail
(
    id                  BIGINT COMMENT '发送记录ID',
    activity_id         BIGINT COMMENT '活动编号',
    product_id          BIGINT COMMENT '商品ID',
    sku_id              BIGINT COMMENT 'SKU ID（商品级活动可为NULL）',
    customer_service_id BIGINT COMMENT '客服ID',
    customer_id         BIGINT COMMENT '消费者ID',
    send_discount       DECIMAL(10, 2) COMMENT '发送的优惠金额',
    valid_period        INT COMMENT '消费者使用有效期（小时）',
    send_time           STRING COMMENT '发送时间',
    remark              STRING COMMENT '备注（内部协同用）',
    is_used             TINYINT COMMENT '是否被使用：0未使用/1已使用',
    use_time            STRING COMMENT '使用时间（未使用可为NULL）',
    raw_data            STRING COMMENT '原始数据JSON串'
) COMMENT '客服发送优惠原始明细表'
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE;

-- 向ods_customer_service_send_detail表插入1000条模拟数据（兼容低版本Hive）
INSERT OVERWRITE TABLE ods_customer_service_send_detail
SELECT id,
       activity_id,
       product_id,
       sku_id,
       customer_service_id,
       customer_id,
       send_discount,
       valid_period,
       send_time,
       remark,
       is_used,
       -- 使用时间（低版本Hive兼容写法：通过时间戳计算）
       CASE
           WHEN is_used = 1 THEN
               from_unixtime(
                       unix_timestamp(send_time) + cast(rand() * valid_period * 3600 AS INT)
               )
           ELSE NULL
           END AS use_time,
       -- 原始数据JSON串
       concat(
               '{"id":', id,
               ',"activity_id":', activity_id,
               ',"product_id":', product_id,
               ',"sku_id":', CASE WHEN sku_id IS NULL THEN 'null' ELSE sku_id END,
               ',"customer_service_id":', customer_service_id,
               ',"customer_id":', customer_id,
               ',"send_discount":', send_discount,
               ',"valid_period":', valid_period,
               ',"is_used":', is_used,
               '}'
       )       AS raw_data
FROM (SELECT row_number() OVER ()                      AS id,
             cast(rand() * 100 + 1 AS BIGINT)          AS activity_id,
             cast(rand() * 1000 + 1000 AS BIGINT)      AS product_id,
             CASE
                 WHEN cast(rand() * 2 AS INT) = 0 THEN NULL
                 ELSE cast(rand() * 5000 + 10000 AS BIGINT)
                 END                                   AS sku_id,
             cast(rand() * 100 + 100 AS BIGINT)        AS customer_service_id,
             cast(rand() * 40000 + 10000 AS BIGINT)    AS customer_id,
             cast(rand() * 5000 + 1 AS DECIMAL(10, 2)) AS send_discount,
             cast(rand() * 23 + 1 AS INT)              AS valid_period,
             -- 发送时间：通过时间戳生成，兼容所有Hive版本
             from_unixtime(
                     unix_timestamp(current_date)
                         - cast(rand() * 30 * 86400 AS INT) -- 30天内随机天数
                         + cast(rand() * 86400 AS INT) -- 当天内随机秒数
             )                                         AS send_time,
             CASE
                 WHEN cast(rand() * 3 AS INT) = 0 THEN '新客转化'
                 WHEN cast(rand() * 3 AS INT) = 1 THEN '老客回馈'
                 ELSE NULL
                 END                                   AS remark,
             cast(rand() * 2 AS TINYINT)               AS is_used
      FROM (
               -- 生成1000条数据的基础序列
               SELECT pos
               FROM (SELECT posexplode(split(space(999), '')) AS (pos, val)) t) temp) data_source;


select *
from ods_customer_service_send_detail;

-- 商品原始信息表
DROP TABLE IF EXISTS ods_product_info;
CREATE TABLE IF NOT EXISTS ods_product_info
(
    product_id     BIGINT COMMENT '商品ID',
    product_name   STRING COMMENT '商品名称',
    price          DECIMAL(10, 2) COMMENT '商品价格',
    red_line_price DECIMAL(10, 2) COMMENT '红线价（默认标价四折）',
    create_time    STRING COMMENT '商品创建时间',
    update_time    STRING COMMENT '商品更新时间',
    raw_data       STRING COMMENT '原始数据JSON串'
) COMMENT '商品原始信息表'
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE;

-- 向ods_product_info表插入1000条模拟数据
INSERT OVERWRITE TABLE ods_product_info
SELECT
    -- 商品ID（1000-2000范围内唯一）
    1000 + row_number() OVER ()                        AS product_id,
    -- 商品名称（随机生成，不超过50字）
    concat(
            CASE cast(rand() * 5 AS INT)
                WHEN 0 THEN '男士'
                WHEN 1 THEN '女士'
                WHEN 2 THEN '儿童'
                WHEN 3 THEN '户外'
                ELSE '家用'
                END,
            CASE cast(rand() * 8 AS INT)
                WHEN 0 THEN 'T恤'
                WHEN 1 THEN '裤子'
                WHEN 2 THEN '鞋子'
                WHEN 3 THEN '背包'
                WHEN 4 THEN '手表'
                WHEN 5 THEN '耳机'
                WHEN 6 THEN '水杯'
                ELSE '帽子'
                END,
            '-',
            cast(row_number() OVER () AS STRING)
    )                                                  AS product_name,
    -- 商品价格（10-2000元随机，保留两位小数）
    cast(rand() * 1990 + 10 AS DECIMAL(10, 2))         AS price,
    -- 红线价（默认是商品标价的四折，符合文档规则）
    cast((rand() * 1990 + 10) * 0.4 AS DECIMAL(10, 2)) AS red_line_price,
    -- 创建时间（近1年内随机生成）
    date_add(current_date, -cast(rand() * 365 AS INT)) AS create_time,
    -- 更新时间（创建时间之后，随机生成）
    date_add(
            date_add(current_date, -cast(rand() * 365 AS INT)),
            cast(rand() * 100 AS INT)
    )                                                  AS update_time,
    -- 原始数据JSON串
    concat(
            '{"product_id":', 1000 + row_number() OVER (),
            ',"product_name":"', concat(
                    CASE cast(rand() * 5 AS INT)
                        WHEN 0 THEN '男士'
                        WHEN 1 THEN '女士'
                        WHEN 2 THEN '儿童'
                        WHEN 3 THEN '户外'
                        ELSE '家用'
                        END,
                    CASE cast(rand() * 8 AS INT)
                        WHEN 0 THEN 'T恤'
                        WHEN 1 THEN '裤子'
                        WHEN 2 THEN '鞋子'
                        WHEN 3 THEN '背包'
                        WHEN 4 THEN '手表'
                        WHEN 5 THEN '耳机'
                        WHEN 6 THEN '水杯'
                        ELSE '帽子'
                        END,
                    '-',
                    cast(row_number() OVER () AS STRING)
                                 ),
            ',"price":', cast(rand() * 1990 + 10 AS DECIMAL(10, 2)),
            ',"red_line_price":', cast((rand() * 1990 + 10) * 0.4 AS DECIMAL(10, 2)),
            '}'
    )                                                  AS raw_data
FROM (
         -- 生成1000条数据的基础序列
         SELECT pos
         FROM (SELECT posexplode(split(space(999), '')) AS (pos, val)) t) temp;

select *
from ods_product_info;

-- SKU原始信息表
DROP TABLE IF EXISTS ods_sku_info;
CREATE TABLE IF NOT EXISTS ods_sku_info
(
    sku_id     BIGINT COMMENT 'SKU ID',
    product_id BIGINT COMMENT '商品ID',
    sku_name   STRING COMMENT 'SKU名称',
    price      DECIMAL(10, 2) COMMENT 'SKU价格',
    stock      INT COMMENT '库存数量',
    raw_data   STRING COMMENT '原始数据JSON串'
) COMMENT 'SKU原始信息表'
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE;

-- 向ods_sku_info表插入1000条模拟数据
INSERT OVERWRITE TABLE ods_sku_info
SELECT
    -- SKU ID（10000-11000范围内唯一）
    10000 + row_number() OVER ()                 AS sku_id,
    -- 商品ID（关联商品表的1000-2000范围，确保每个商品对应1-3个SKU）
    1000 + cast(row_number() OVER () / 3 AS INT) AS product_id,
    -- SKU名称（基于商品ID和属性生成，如颜色、尺码）
    concat(
            '商品', 1000 + cast(row_number() OVER () / 3 AS INT),
            '-',
            CASE cast(rand() * 5 AS INT) -- 随机颜色
                WHEN 0 THEN '红色'
                WHEN 1 THEN '蓝色'
                WHEN 2 THEN '黑色'
                WHEN 3 THEN '白色'
                ELSE '灰色'
                END,
            '-',
            CASE cast(rand() * 4 AS INT) -- 随机尺码
                WHEN 0 THEN 'S'
                WHEN 1 THEN 'M'
                WHEN 2 THEN 'L'
                ELSE 'XL'
                END
    )                                            AS sku_name,
    -- SKU价格（基于商品价格浮动±10%，保留两位小数）
    cast(
            (100 + rand() * 1900) * (0.9 + rand() * 0.2) -- 100-2000元基础上浮动
        AS DECIMAL(10, 2)
    )                                            AS price,
    -- 库存数量（10-1000的非负整数，符合实际业务场景）
    cast(rand() * 990 + 10 AS INT)               AS stock,
    -- 原始数据JSON串
    concat(
            '{"sku_id":', 10000 + row_number() OVER (),
            ',"product_id":', 1000 + cast(row_number() OVER () / 3 AS INT),
            ',"sku_name":"', concat(
                    '商品', 1000 + cast(row_number() OVER () / 3 AS INT),
                    '-',
                    CASE cast(rand() * 5 AS INT)
                        WHEN 0 THEN '红色'
                        WHEN 1 THEN '蓝色'
                        WHEN 2 THEN '黑色'
                        WHEN 3 THEN '白色'
                        ELSE '灰色'
                        END,
                    '-',
                    CASE cast(rand() * 4 AS INT)
                        WHEN 0 THEN 'S'
                        WHEN 1 THEN 'M'
                        WHEN 2 THEN 'L'
                        ELSE 'XL'
                        END
                             ),
            ',"price":', cast((100 + rand() * 1900) * (0.9 + rand() * 0.2) AS DECIMAL(10, 2)),
            ',"stock":', cast(rand() * 990 + 10 AS INT),
            '}'
    )                                            AS raw_data
FROM (
         -- 生成1000条数据的基础序列
         SELECT pos
         FROM (SELECT posexplode(split(space(999), '')) AS (pos, val)) t) temp;

select *
from ods_sku_info;