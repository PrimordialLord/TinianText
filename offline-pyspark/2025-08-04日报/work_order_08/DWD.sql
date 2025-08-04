USE work_order_08;

-- 活动明细事实表
DROP TABLE IF EXISTS dwd_activity_info;
CREATE TABLE IF NOT EXISTS dwd_activity_info
(
    activity_id    BIGINT COMMENT '活动编号',
    activity_name  STRING COMMENT '活动名称（不超过10字）',
    activity_level STRING COMMENT '活动级别：商品级/SKU级',
    activity_type  STRING COMMENT '优惠类型：固定优惠/自定义优惠',
    start_time     TIMESTAMP COMMENT '活动开始时间',
    end_time       TIMESTAMP COMMENT '活动结束时间',
    max_discount   DECIMAL(10, 2) COMMENT '最大优惠金额（自定义优惠时使用）',
    status         STRING COMMENT '活动状态：进行中/已结束/未开始/已暂停',
    create_time    TIMESTAMP COMMENT '创建时间',
    update_time    TIMESTAMP COMMENT '更新时间'
) COMMENT '活动明细事实表'
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\t'
    STORED AS ORC;

-- 从ODS层ods_activity_info提取数据并清洗转换后插入DWD层
INSERT OVERWRITE TABLE dwd_activity_info
SELECT
    -- 活动编号直接继承，确保唯一性
    o.activity_id,
    -- 清洗活动名称：截取超长名称至10字以内
    CASE
        WHEN length(o.activity_name) > 10 THEN substr(o.activity_name, 1, 10)
        ELSE o.activity_name
        END                                                    AS activity_name,
    -- 标准化活动级别：仅保留商品级/SKU级（处理异常值）
    CASE
        WHEN o.activity_level NOT IN ('商品级', 'SKU级') THEN '商品级' -- 异常值默认商品级
        ELSE o.activity_level
        END                                                    AS activity_level,
    -- 标准化优惠类型：仅保留固定优惠/自定义优惠（处理异常值）
    CASE
        WHEN o.activity_type NOT IN ('固定优惠', '自定义优惠') THEN '固定优惠' -- 异常值默认固定优惠
        ELSE o.activity_type
        END                                                    AS activity_type,
    -- 转换开始时间为TIMESTAMP（处理无效时间格式）
    CASE
        WHEN regexp_extract(o.start_time, '\\d{4}-\\d{2}-\\d{2}', 0) = '' -- 无效日期格式
            THEN from_unixtime(unix_timestamp()) -- 用当前时间替代
        ELSE from_unixtime(unix_timestamp(o.start_time, 'yyyy-MM-dd')) -- 转换为标准时间
        END                                                    AS start_time,
    -- 转换结束时间为TIMESTAMP（确保晚于开始时间）
    CASE
        -- 结束时间早于开始时间时，自动修正为开始时间后30天
        WHEN unix_timestamp(o.end_time, 'yyyy-MM-dd') < unix_timestamp(o.start_time, 'yyyy-MM-dd')
            THEN from_unixtime(unix_timestamp(o.start_time, 'yyyy-MM-dd') + 30 * 86400)
        ELSE from_unixtime(unix_timestamp(o.end_time, 'yyyy-MM-dd'))
        END                                                    AS end_time,
    -- 清洗最大优惠金额（仅自定义优惠有效，且不超过5000元）
    CASE
        WHEN o.activity_type = '自定义优惠'
            THEN CASE
                     WHEN o.max_discount IS NULL OR o.max_discount <= 0 THEN 100.00 -- 异常值默认100元
                     WHEN o.max_discount > 5000 THEN 5000.00 -- 超限值截断为5000
                     ELSE o.max_discount
            END
        ELSE NULL -- 固定优惠无最大金额
        END                                                    AS max_discount,
    -- 标准化活动状态（仅保留4种合法状态）
    CASE
        WHEN o.status NOT IN ('进行中', '已结束', '未开始', '已暂停') THEN '未开始' -- 异常值默认未开始
        ELSE o.status
        END                                                    AS status,
    -- 转换创建时间为TIMESTAMP
    from_unixtime(unix_timestamp(o.create_time, 'yyyy-MM-dd')) AS create_time,
    -- 转换更新时间为TIMESTAMP（确保不早于创建时间）
    CASE
        WHEN unix_timestamp(o.update_time, 'yyyy-MM-dd') < unix_timestamp(o.create_time, 'yyyy-MM-dd')
            THEN from_unixtime(unix_timestamp(o.create_time, 'yyyy-MM-dd')) -- 异常值取创建时间
        ELSE from_unixtime(unix_timestamp(o.update_time, 'yyyy-MM-dd'))
        END                                                    AS update_time
FROM ods_activity_info o
-- 可根据需要添加过滤条件（如仅处理有效活动）
WHERE o.activity_id IS NOT NULL; -- 过滤掉活动编号为空的无效数据

select *
from dwd_activity_info;

-- 商品活动关联明细事实表
CREATE TABLE IF NOT EXISTS dwd_product_activity_rel
(
    id              BIGINT COMMENT '关联记录ID',
    activity_id     BIGINT COMMENT '活动编号',
    product_id      BIGINT COMMENT '商品ID',
    sku_id          BIGINT COMMENT 'SKU ID（商品级活动为NULL）',
    discount_amount DECIMAL(10, 2) COMMENT '优惠金额（自定义优惠时为NULL）',
    limit_purchase  INT COMMENT '每人限购次数',
    is_published    TINYINT COMMENT '是否发布：0未发布/1已发布',
    add_time        TIMESTAMP COMMENT '添加时间',
    remove_time     TIMESTAMP COMMENT '移出时间（未移出为NULL）'
) COMMENT '商品活动关联明细事实表'
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\t'
    STORED AS ORC;

-- 从ODS层关联生成DWD层数据（修正版）
INSERT OVERWRITE TABLE dwd_product_activity_rel
SELECT
    -- 关联记录ID直接继承
    rel.id,
    -- 活动编号（关联活动表验证合法性）
    rel.activity_id,
    -- 商品ID（关联商品表验证存在性）
    rel.product_id,
    -- SKU ID（商品级活动强制为NULL，SKU级活动保留原始值）
    CASE
        WHEN act.activity_level = '商品级' THEN NULL
        ELSE rel.sku_id
        END AS sku_id,
    -- 优惠金额（固定优惠有效，自定义优惠位置空）
    CASE
        WHEN act.activity_type = '固定优惠' THEN
            CASE
                WHEN rel.discount_amount <= 0 THEN 10.00 -- 修正异常值
                WHEN rel.discount_amount > prod.red_line_price THEN prod.red_line_price * 0.9 -- 不超过红线价90%
                ELSE rel.discount_amount
                END
        ELSE NULL -- 自定义优惠时为空
        END AS discount_amount,
    -- 限购次数（修正为1-5次的合理范围）
    CASE
        WHEN rel.limit_purchase < 1 THEN 1
        WHEN rel.limit_purchase > 5 THEN 5
        ELSE rel.limit_purchase
        END AS limit_purchase,
    -- 是否发布（标准化为0/1）
    CASE
        WHEN rel.is_published NOT IN (0, 1) THEN 0 -- 异常值默认未发布
        ELSE rel.is_published
        END AS is_published,
    -- 添加时间（转换为TIMESTAMP，确保在活动开始后）
    CASE
        WHEN unix_timestamp(rel.add_time) < unix_timestamp(act.start_time)
            THEN from_unixtime(unix_timestamp(act.start_time)) -- 早于活动开始时间则修正
        ELSE from_unixtime(unix_timestamp(rel.add_time))
        END AS add_time,
    -- 移出时间（已发布商品不允许有移出时间）
    CASE
        WHEN rel.is_published = 1 THEN NULL -- 已发布商品强制为空
        WHEN rel.remove_time IS NOT NULL AND unix_timestamp(rel.remove_time) < unix_timestamp(rel.add_time)
            THEN from_unixtime(unix_timestamp(rel.add_time)) -- 早于添加时间则修正
        ELSE from_unixtime(unix_timestamp(rel.remove_time))
        END AS remove_time
-- 主表为商品活动关联原始表
FROM ods_product_activity_rel rel
-- 关联活动表获取活动属性（用于逻辑校验）
         LEFT JOIN ods_activity_info act
                   ON rel.activity_id = act.activity_id
-- 关联商品表获取红线价（用于优惠金额校验）
         LEFT JOIN ods_product_info prod
                   ON rel.product_id = prod.product_id
-- 过滤条件：确保活动和商品存在
WHERE act.activity_id IS NOT NULL
  AND prod.product_id IS NOT NULL
  -- 对于SKU级活动，确保SKU ID不为空（不依赖SKU表）
  AND NOT (act.activity_level = 'SKU级' AND rel.sku_id IS NULL);

select *
from dwd_product_activity_rel;
-- 客服发送优惠明细事实表
CREATE TABLE IF NOT EXISTS dwd_customer_service_send_detail
(
    id                  BIGINT COMMENT '发送记录ID',
    activity_id         BIGINT COMMENT '活动编号',
    product_id          BIGINT COMMENT '商品ID',
    sku_id              BIGINT COMMENT 'SKU ID（商品级活动为NULL）',
    customer_service_id BIGINT COMMENT '客服ID',
    customer_id         BIGINT COMMENT '消费者ID',
    send_discount       DECIMAL(10, 2) COMMENT '发送的优惠金额',
    valid_period        INT COMMENT '消费者使用有效期（小时）',
    send_time           TIMESTAMP COMMENT '发送时间',
    remark              STRING COMMENT '备注（内部协同用）',
    is_used             TINYINT COMMENT '是否被使用：0未使用/1已使用',
    use_time            TIMESTAMP COMMENT '使用时间（未使用为NULL）'
) COMMENT '客服发送优惠明细事实表'
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\t'
    STORED AS ORC;

-- 从ODS层提取并清洗数据插入DWD层
INSERT OVERWRITE TABLE dwd_customer_service_send_detail
SELECT
    -- 发送记录ID直接继承
    o.id,
    -- 活动编号（关联活动表验证合法性）
    o.activity_id,
    -- 商品ID（关联商品表验证存在性）
    o.product_id,
    -- SKU ID（商品级活动强制为NULL，与活动级别匹配）
    CASE
        WHEN act.activity_level = '商品级' THEN NULL
        ELSE o.sku_id
        END AS sku_id,
    -- 客服ID（清洗异常值，确保为正数）
    CASE
        WHEN o.customer_service_id <= 0 THEN cast(rand() * 100 + 100 AS BIGINT) -- 异常值替换为合理范围
        ELSE o.customer_service_id
        END AS customer_service_id,
    -- 消费者ID（清洗异常值，确保为正数）
    CASE
        WHEN o.customer_id <= 0 THEN cast(rand() * 40000 + 10000 AS BIGINT) -- 异常值替换为合理范围
        ELSE o.customer_id
        END AS customer_id,
    -- 发送的优惠金额（校验不超过活动最大优惠和商品红线价）
    CASE
        -- 自定义优惠不超过活动最大金额
        WHEN act.activity_type = '自定义优惠' AND o.send_discount > act.max_discount
            THEN act.max_discount
        -- 固定优惠不超过商品红线价90%
        WHEN act.activity_type = '固定优惠' AND o.send_discount > prod.red_line_price * 0.9
            THEN prod.red_line_price * 0.9
        -- 修正负数或零值
        WHEN o.send_discount <= 0 THEN 10.00
        ELSE o.send_discount
        END AS send_discount,
    -- 有效期（限制在1-72小时合理范围）
    CASE
        WHEN o.valid_period < 1 THEN 24 -- 最小24小时
        WHEN o.valid_period > 72 THEN 72 -- 最大72小时
        ELSE o.valid_period
        END AS valid_period,
    -- 发送时间（转换为TIMESTAMP，确保在活动期间内）
    CASE
        -- 早于活动开始时间则修正为活动开始时间
        WHEN unix_timestamp(o.send_time) < unix_timestamp(act.start_time)
            THEN from_unixtime(unix_timestamp(act.start_time))
        -- 晚于活动结束时间则修正为活动结束时间
        WHEN unix_timestamp(o.send_time) > unix_timestamp(act.end_time)
            THEN from_unixtime(unix_timestamp(act.end_time))
        ELSE from_unixtime(unix_timestamp(o.send_time))
        END AS send_time,
    -- 备注（清洗空值和超长内容）
    CASE
        WHEN o.remark IS NULL THEN '无备注'
        WHEN length(o.remark) > 50 THEN substr(o.remark, 1, 50) -- 超长截断
        ELSE o.remark
        END AS remark,
    -- 是否被使用（标准化为0/1）
    CASE
        WHEN o.is_used NOT IN (0, 1) THEN 0 -- 异常值默认未使用
        ELSE o.is_used
        END AS is_used,
    -- 使用时间（未使用则为NULL，且需在发送时间+有效期内）
    CASE
        WHEN o.is_used = 0 THEN NULL -- 未使用强制为空
    -- 修正早于发送时间的情况
        WHEN unix_timestamp(o.use_time) < unix_timestamp(o.send_time)
            THEN from_unixtime(unix_timestamp(o.send_time))
        -- 修正超过有效期的情况
        WHEN unix_timestamp(o.use_time) > unix_timestamp(o.send_time) + o.valid_period * 3600
            THEN from_unixtime(unix_timestamp(o.send_time) + o.valid_period * 3600)
        ELSE from_unixtime(unix_timestamp(o.use_time))
        END AS use_time
-- 主表为客服发送优惠原始明细表
FROM ods_customer_service_send_detail o
-- 关联活动表获取活动属性（用于金额和时间校验）
         LEFT JOIN ods_activity_info act
                   ON o.activity_id = act.activity_id
-- 关联商品表获取红线价（用于优惠金额校验）
         LEFT JOIN ods_product_info prod
                   ON o.product_id = prod.product_id
-- 过滤条件：确保活动和商品存在
WHERE act.activity_id IS NOT NULL
  AND prod.product_id IS NOT NULL
  -- SKU级活动必须有SKU ID
  AND NOT (act.activity_level = 'SKU级' AND o.sku_id IS NULL);

select *
from dwd_customer_service_send_detail;

-- 商品明细事实表
CREATE TABLE IF NOT EXISTS dwd_product_info
(
    product_id     BIGINT COMMENT '商品ID',
    product_name   STRING COMMENT '商品名称',
    price          DECIMAL(10, 2) COMMENT '商品价格',
    red_line_price DECIMAL(10, 2) COMMENT '红线价（默认标价四折）',
    create_time    TIMESTAMP COMMENT '商品创建时间',
    update_time    TIMESTAMP COMMENT '商品更新时间'
) COMMENT '商品明细事实表'
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\t'
    STORED AS ORC;

-- 从ODS层提取并清洗数据插入DWD层
INSERT OVERWRITE TABLE dwd_product_info
SELECT
    -- 商品ID直接继承
    o.product_id,
    -- 商品名称清洗（去除特殊字符、截断超长名称）
    CASE
        WHEN o.product_name IS NULL THEN CONCAT('未知商品_', o.product_id) -- 补充空名称
        WHEN LENGTH(o.product_name) > 100 THEN SUBSTR(o.product_name, 1, 100) -- 超长截断
        ELSE REGEXP_REPLACE(o.product_name, '[^a-zA-Z0-9\u4e00-\u9fa5]', '') -- 去除特殊字符
        END AS product_name,
    -- 商品价格清洗（确保为正数且合理范围）
    CASE
        WHEN o.price <= 0 THEN 99.99 -- 修正负数或零值
        WHEN o.price > 10000 THEN 10000.00 -- 超上限截断
        ELSE o.price
        END AS price,
    -- 红线价清洗（确保不高于商品价格，默认四折）
    CASE
        WHEN o.red_line_price > price THEN ROUND(price * 0.4, 2) -- 高于售价时修正为四折
        WHEN o.red_line_price <= 0 THEN ROUND(price * 0.4, 2) -- 异常值修正为四折
        ELSE o.red_line_price
        END AS red_line_price,
    -- 创建时间转换（确保有效时间格式）
    CASE
        WHEN REGEXP_EXTRACT(o.create_time, '\\d{4}-\\d{2}-\\d{2}', 0) = ''
            THEN FROM_UNIXTIME(UNIX_TIMESTAMP()) -- 无效时间用当前时间
        ELSE FROM_UNIXTIME(UNIX_TIMESTAMP(o.create_time, 'yyyy-MM-dd'))
        END AS create_time,
    -- 更新时间转换（确保不早于创建时间）
    CASE
        WHEN UNIX_TIMESTAMP(o.update_time, 'yyyy-MM-dd') < UNIX_TIMESTAMP(o.create_time, 'yyyy-MM-dd')
            THEN FROM_UNIXTIME(UNIX_TIMESTAMP(o.create_time, 'yyyy-MM-dd')) -- 早于早于创建时间
        WHEN REGEXP_EXTRACT(o.update_time, '\\d{4}-\\d{2}-\\d{2}', 0) = ''
            THEN FROM_UNIXTIME(UNIX_TIMESTAMP(o.create_time, 'yyyy-MM-dd')) -- 无效时间用创建时间
        ELSE FROM_UNIXTIME(UNIX_TIMESTAMP(o.update_time, 'yyyy-MM-dd'))
        END AS update_time
FROM ods_product_info o
-- 过滤条件：确保商品ID有效
WHERE o.product_id IS NOT NULL
  AND o.product_id > 0; -- 排除无效商品ID


select *
from dwd_product_info;

-- SKU明细事实表
CREATE TABLE IF NOT EXISTS dwd_sku_info
(
    sku_id     BIGINT COMMENT 'SKU ID',
    product_id BIGINT COMMENT '商品ID',
    sku_name   STRING COMMENT 'SKU名称',
    price      DECIMAL(10, 2) COMMENT 'SKU价格',
    stock      INT COMMENT '库存数量'
) COMMENT 'SKU明细事实表'
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\t'
    STORED AS ORC;

-- 从替代数据源提取并清洗数据插入DWD层（修正版）
INSERT OVERWRITE TABLE dwd_sku_info
SELECT
    -- 生成SKU ID（10000开始的自增ID）
    10000 + row_number() OVER ()                           AS sku_id,
    -- 商品ID（关联商品表确保存在）
    p.product_id,
    -- 生成规范的SKU名称
    CONCAT(
            p.product_name,
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
                WHEN 2 THEN 'L'
                ELSE 'XL'
                END
    )                                                      AS sku_name,
    -- 基于商品价格生成合理的SKU价格
    cast(p.price * (0.9 + rand() * 0.6) AS DECIMAL(10, 2)) AS price,
    -- 生成合理的库存数量
    cast(rand() * 990 + 10 AS INT)                         AS stock
-- 从商品表获取基础数据，为每个商品生成1-3个SKU
FROM dwd_product_info p
-- 生成每个商品对应3个SKU的序列
         LATERAL VIEW posexplode(split(space(2), '')) t AS pos, val
-- 确保商品ID有效
WHERE p.product_id IS NOT NULL;


select *
from dwd_sku_info;