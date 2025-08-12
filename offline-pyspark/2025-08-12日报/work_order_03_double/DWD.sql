USE rewrite_work_order_03;

-- todo 1. 商品行为明细事实表（dwd_product_behavior_detail）
DROP TABLE IF EXISTS dwd_product_behavior_detail;
CREATE TABLE IF NOT EXISTS dwd_product_behavior_detail
(
    user_id       STRING COMMENT '用户ID',
    product_id    STRING COMMENT '商品ID',
    sku_id        STRING COMMENT 'SKU ID',
    behavior_type STRING COMMENT '行为类型（click-点击、view-浏览、pay-支付、search-搜索、comment-评价）',
    behavior_time STRING COMMENT '行为时间（yyyy-MM-dd HH:mm:ss）',
    behavior_date STRING COMMENT '行为日期（yyyy-MM-dd）',
    hour          STRING COMMENT '行为小时（00-23）',
    channel_id    STRING COMMENT '渠道ID（关联dim_channel）',
    keyword       STRING COMMENT '搜索关键词（行为类型为search时有效）',
    content_id    STRING COMMENT '内容ID（从内容跳转时有效）',
    stay_time     INT COMMENT '停留时间（秒，浏览行为时有效）'
) COMMENT '商品行为明细事实表'
    PARTITIONED BY (dt STRING COMMENT '分区日期（yyyy-MM-dd）')
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\t'
    STORED AS ORC;

INSERT INTO TABLE dwd_product_behavior_detail PARTITION (dt='2025-01-26')
SELECT
    -- 用户ID（直接取自行为日志）
    log.user_id,
    -- 商品ID（直接取自行为日志）
    log.product_id,
    -- SKU ID（直接取自行为日志）
    log.sku_id,
    -- 行为类型（直接取自行为日志）
    log.behavior_type,
    -- 行为时间（原始时间，精确到秒）
    log.behavior_time,
    -- 行为日期（从行为时间提取yyyy-MM-dd）
    substr(log.behavior_time, 1, 10) AS behavior_date,
    -- 行为小时（从行为时间提取00-23）
    substr(log.behavior_time, 12, 2) AS hour,
    -- 渠道ID（关联渠道维度表获取）
    channel.channel_id,
    -- 搜索关键词（仅search行为有效）
    log.keyword,
    -- 内容ID（从内容跳转时有效）
    log.content_id,
    -- 停留时间（仅view行为有效）
    log.stay_time
FROM
    -- ODS层用户行为日志表（源数据）
    ods_user_behavior_log log
-- 关联DIM层渠道维度表，补充渠道ID
        LEFT JOIN dim_channel channel
                  ON log.channel = channel.channel_name
                      AND channel.dt = '2025-01-26'  -- 关联当日渠道维度
WHERE
    log.dt = '2025-01-26';  -- 取当日行为数据

select * from dwd_product_behavior_detail;
-- todo 2. 订单明细事实表（dwd_order_detail）
DROP TABLE IF EXISTS dwd_order_detail;
CREATE TABLE IF NOT EXISTS dwd_order_detail
(
    order_id     STRING COMMENT '订单ID',
    user_id      STRING COMMENT '用户ID',
    product_id   STRING COMMENT '商品ID',
    sku_id       STRING COMMENT 'SKU ID',
    order_time   STRING COMMENT '下单时间（yyyy-MM-dd HH:mm:ss）',
    order_date   STRING COMMENT '下单日期（yyyy-MM-dd）',
    pay_time     STRING COMMENT '支付时间（yyyy-MM-dd HH:mm:ss）',
    order_amount DECIMAL(16, 2) COMMENT '订单金额',
    order_count  INT COMMENT '购买数量',
    is_paid      INT COMMENT '是否支付（1-是，0-否）'
) COMMENT '订单明细事实表'
    PARTITIONED BY (dt STRING COMMENT '分区日期（yyyy-MM-dd）')
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\t'
    STORED AS ORC;

INSERT INTO TABLE dwd_order_detail PARTITION (dt='2025-01-26')
SELECT
    -- 订单ID（取自ODS层订单表）
    order_info.order_id,
    -- 用户ID（取自ODS层订单表，可关联dim_user）
    order_info.user_id,
    -- 商品ID（取自ODS层订单表，可关联dim_product）
    order_info.product_id,
    -- SKU ID（取自ODS层订单表）
    order_info.sku_id,
    -- 下单时间（原始时间，精确到秒）
    order_info.order_time,
    -- 下单日期（从下单时间提取yyyy-MM-dd）
    substr(order_info.order_time, 1, 10) AS order_date,
    -- 支付时间（取自ODS层订单表，未支付则为空）
    order_info.pay_time,
    -- 订单金额（取自ODS层订单表）
    order_info.order_amount,
    -- 购买数量（取自ODS层订单表）
    order_info.order_count,
    -- 是否支付（1-已支付，0-未支付/已取消）
    CASE
        WHEN order_info.order_status = '已支付' THEN 1
        ELSE 0
        END AS is_paid
FROM
    -- ODS层订单原始表（源数据）
    ods_order_info order_info
WHERE
    order_info.dt = '2025-01-26';  -- 取当日订单数据

select * from dwd_order_detail;
-- todo 3. 评价明细事实表（dwd_comment_detail）
DROP TABLE IF EXISTS dwd_comment_detail;
CREATE TABLE IF NOT EXISTS dwd_comment_detail
(
    comment_id   STRING COMMENT '评价ID',
    product_id   STRING COMMENT '商品ID',
    sku_id       STRING COMMENT 'SKU ID',
    user_id      STRING COMMENT '用户ID',
    score        INT COMMENT '评分（1-5星）',
    content      STRING COMMENT '评价内容（脱敏处理）',
    comment_time STRING COMMENT '评价时间（yyyy-MM-dd HH:mm:ss）',
    comment_date STRING COMMENT '评价日期（yyyy-MM-dd）',
    is_positive  INT COMMENT '是否正面评价（0-否，1-是）',
    is_active    INT COMMENT '是否主动评价（0-否，1-是）',
    user_type    STRING COMMENT '用户类型（new-新用户，old-老用户）'
) COMMENT '评价明细事实表'
    PARTITIONED BY (dt STRING COMMENT '分区日期（yyyy-MM-dd）')
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\t'
    STORED AS ORC;

INSERT INTO TABLE dwd_comment_detail PARTITION (dt='2025-01-26')
SELECT
    -- 评价ID（取自ODS层评价表）
    comment_info.comment_id,
    -- 商品ID（取自ODS层评价表，可关联dim_product）
    comment_info.product_id,
    -- SKU ID（取自ODS层评价表）
    comment_info.sku_id,
    -- 用户ID（取自ODS层评价表，可关联dim_user）
    comment_info.user_id,
    -- 评分（1-5星，直接取自ODS层）
    comment_info.score,
    -- 评价内容（此处假设已做脱敏处理，直接复用）
    comment_info.content,
    -- 评价时间（原始时间，精确到秒）
    comment_info.create_time AS comment_time,
    -- 评价日期（从评价时间提取yyyy-MM-dd）
    substr(comment_info.create_time, 1, 10) AS comment_date,
    -- 是否正面评价（0-否，1-是，直接取自ODS层）
    comment_info.is_positive,
    -- 是否主动评价（0-否，1-是，直接取自ODS层）
    comment_info.is_active,
    -- 用户类型（new-新用户，old-老用户，直接取自ODS层）
    comment_info.user_type
FROM
    -- ODS层评价原始表（源数据）
    ods_comment_info comment_info
-- 关联商品维度表，过滤无效商品（仅保留有效商品的评价）
        LEFT JOIN dim_product product
                  ON comment_info.product_id = product.product_id
                      AND product.dt = '2025-01-26'  -- 关联当日商品维度
WHERE
    comment_info.dt = '2025-01-26'  -- 取当日评价数据
  AND product.is_valid = 1;  -- 仅保留有效商品的评价

select * from dwd_comment_detail;
-- todo 4. 内容营销明细事实表（dwd_content_marketing_detail）
DROP TABLE IF EXISTS dwd_content_marketing_detail;
CREATE TABLE IF NOT EXISTS dwd_content_marketing_detail
(
    content_id       STRING COMMENT '内容ID',
    product_id       STRING COMMENT '关联商品ID',
    content_type     STRING COMMENT '内容类型（live-直播、video-短视频、article-图文）',
    publish_time     STRING COMMENT '发布时间（yyyy-MM-dd HH:mm:ss）',
    publish_date     STRING COMMENT '发布日期（yyyy-MM-dd）',
    view_count       INT COMMENT '观看量',
    click_count      INT COMMENT '点击量',
    conversion_count INT COMMENT '转化量',
    rank_num         INT COMMENT '当日TOP排名（1-50）'
) COMMENT '内容营销明细事实表'
    PARTITIONED BY (dt STRING COMMENT '分区日期（yyyy-MM-dd）')
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\t'
    STORED AS ORC;

INSERT INTO TABLE dwd_content_marketing_detail PARTITION (dt='2025-01-26')
SELECT
    -- 直接引用子查询输出的列，而非内部表别名
    t.content_id,
    t.product_id,
    t.content_type,
    t.publish_time,
    -- 发布日期（从发布时间提取yyyy-MM-dd）
    substr(t.publish_time, 1, 10) AS publish_date,
    t.view_count,
    t.click_count,
    t.conversion_count,
    -- 当日TOP排名（按转化量降序排列，取1-50名，其余为0）
    CASE
        WHEN t.rank_num <= 50 THEN t.rank_num
        ELSE 0
        END AS rank_num
FROM (
         -- 子查询：关联商品维度并计算当日排名
         SELECT
             c.content_id,
             c.product_id,
             c.content_type,
             c.publish_time,
             c.view_count,
             c.click_count,
             c.conversion_count,
             -- 按转化量降序排列，生成当日排名
             row_number() OVER (ORDER BY c.conversion_count DESC) AS rank_num
         FROM
             ods_content_info c
                 -- 关联商品维度表，过滤无效商品的内容
                 LEFT JOIN dim_product p
                           ON c.product_id = p.product_id
                               AND p.dt = '2025-01-26'
         WHERE
             c.dt = '2025-01-26'  -- 取当日内容数据
           AND p.is_valid = 1  -- 仅保留有效商品的内容
     ) t;  -- 子查询别名为t，外部查询通过t引用列

select * from dwd_content_marketing_detail;
-- todo 5. 运营动作明细事实表（dwd_operation_action_detail）
DROP TABLE IF EXISTS dwd_operation_action_detail;
CREATE TABLE IF NOT EXISTS dwd_operation_action_detail
(
    action_id     STRING COMMENT '运营动作ID',
    product_id    STRING COMMENT '商品ID',
    action_type   STRING COMMENT '动作类型（new_user_discount-新客折扣、installment_free-分期免息）',
    action_time   STRING COMMENT '动作执行时间（yyyy-MM-dd HH:mm:ss）',
    action_date   STRING COMMENT '动作执行日期（yyyy-MM-dd）',
    action_params STRING COMMENT '动作参数（JSON格式）'
) COMMENT '运营动作明细事实表'
    PARTITIONED BY (dt STRING COMMENT '分区日期（yyyy-MM-dd）')
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\t'
    STORED AS ORC;

INSERT INTO TABLE dwd_operation_action_detail PARTITION (dt='2025-01-26')
SELECT
    -- 运营动作ID（取自ODS层运营动作表）
    t.action_id,
    -- 商品ID（取自ODS层，可关联dim_product）
    t.product_id,
    -- 动作类型（直接复用ODS层的类型定义）
    t.action_type,
    -- 动作执行时间（原始时间，精确到秒）
    t.action_time,
    -- 动作执行日期（从执行时间提取yyyy-MM-dd）
    substr(t.action_time, 1, 10) AS action_date,
    -- 动作参数（JSON格式，直接复用）
    t.action_params
FROM (
         -- 子查询：关联商品维度表，过滤无效商品的运营动作
         SELECT
             op.action_id,
             op.product_id,
             op.action_type,
             op.action_time,
             op.action_params
         FROM
             ods_operation_action op
                 -- 关联商品维度表，确保仅保留有效商品的运营动作
                 LEFT JOIN dim_product p
                           ON op.product_id = p.product_id
                               AND p.dt = '2025-01-26'  -- 关联当日商品维度
         WHERE
             op.dt = '2025-01-26'  -- 取当日运营动作数据
           AND p.is_valid = 1  -- 过滤条件：仅保留有效商品（未删除）
     ) t;

select * from dwd_operation_action_detail;