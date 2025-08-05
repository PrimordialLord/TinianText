-- todo DWD层：对ODS层数据清洗、转换和关联，形成明细数据
USE work_order_01_double;
-- todo 商品访问明细事实表
DROP TABLE IF EXISTS dwd_commodity_visit_detail;
CREATE TABLE IF NOT EXISTS dwd_commodity_visit_detail
(
    id
    BIGINT
    COMMENT
    '记录唯一标识',
    commodity_id
    BIGINT
    COMMENT
    '商品ID',
    commodity_name
    STRING
    COMMENT
    '商品名称',
    category_id
    BIGINT
    COMMENT
    '类目ID',
    category_name
    STRING
    COMMENT
    '类目名称',
    visitor_id
    BIGINT
    COMMENT
    '访客ID',
    visit_time
    TIMESTAMP
    COMMENT
    '访问时间',
    visit_date
    DATE
    COMMENT
    '访问日期',
    terminal_type
    TINYINT
    COMMENT
    '终端类型（1-PC端，2-无线端）',
    terminal_name
    STRING
    COMMENT
    '终端名称',
    stay_duration
    INT
    COMMENT
    '停留时长（秒）',
    is_click
    TINYINT
    COMMENT
    '是否有点击行为（0-否，1-是）',
    is_micro_detail
    TINYINT
    COMMENT
    '是否浏览微详情（0-否，1-是）',
    micro_detail_stay
    INT
    COMMENT
    '微详情停留时长（秒）',
    etl_time
    TIMESTAMP
    COMMENT
    'ETL处理时间'
) COMMENT '商品访问明细事实表'
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\t'
    STORED AS ORC;

-- 生成商品访问明细事实表数据
INSERT INTO TABLE dwd_commodity_visit_detail
SELECT
    ovb.id AS id,  -- 记录唯一标识
    ovb.commodity_id AS commodity_id,  -- 商品ID
    obi.commodity_name AS commodity_name,  -- 商品名称（关联商品基础信息表）
    obi.category_id AS category_id,  -- 类目ID（关联商品基础信息表）
    ci.category_name AS category_name,  -- 类目名称（关联类目信息表）
    ovb.visitor_id AS visitor_id,  -- 访客ID
    ovb.visit_time AS visit_time,  -- 访问时间
    to_date(ovb.visit_time) AS visit_date,  -- 访问日期
    ovb.terminal_type AS terminal_type,  -- 终端类型
    CASE ovb.terminal_type WHEN 1 THEN 'PC端' WHEN 2 THEN '无线端' END AS terminal_name,  -- 终端名称
    ovb.stay_duration AS stay_duration,  -- 停留时长
    ovb.is_click AS is_click,  -- 是否有点击行为
    ovb.is_micro_detail AS is_micro_detail,  -- 是否浏览微详情
    ovb.micro_detail_stay AS micro_detail_stay,  -- 微详情停留时长
    current_timestamp() AS etl_time  -- ETL处理时间
FROM ods_commodity_visit_behavior ovb
-- 关联商品基础信息表获取商品名称和类目ID
         LEFT JOIN ods_commodity_base_info obi
                   ON ovb.commodity_id = obi.commodity_id
-- 关联类目信息表获取类目名称
         LEFT JOIN ods_category_info ci
                   ON obi.category_id = ci.category_id;

select * from dwd_commodity_visit_detail;
-- todo 商品收藏加购明细事实表
DROP TABLE IF EXISTS dwd_commodity_collect_cart_detail;
CREATE TABLE IF NOT EXISTS dwd_commodity_collect_cart_detail
(
    id
    BIGINT
    COMMENT
    '记录唯一标识',
    commodity_id
    BIGINT
    COMMENT
    '商品ID',
    commodity_name
    STRING
    COMMENT
    '商品名称',
    category_id
    BIGINT
    COMMENT
    '类目ID',
    user_id
    BIGINT
    COMMENT
    '用户ID',
    behavior_type
    TINYINT
    COMMENT
    '行为类型（1-收藏，2-加购）',
    behavior_type_name
    STRING
    COMMENT
    '行为类型名称',
    behavior_time
    TIMESTAMP
    COMMENT
    '行为时间',
    behavior_date
    DATE
    COMMENT
    '行为日期',
    add_cart_quantity
    INT
    COMMENT
    '加购件数',
    etl_time
    TIMESTAMP
    COMMENT
    'ETL处理时间'
) COMMENT '商品收藏加购明细事实表'
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\t'
    STORED AS ORC;

-- 生成商品收藏加购明细事实表数据
INSERT INTO TABLE dwd_commodity_collect_cart_detail
SELECT
    occ.id AS id,  -- 记录唯一标识
    occ.commodity_id AS commodity_id,  -- 商品ID
    obi.commodity_name AS commodity_name,  -- 商品名称（关联商品基础信息表）
    obi.category_id AS category_id,  -- 类目ID（关联商品基础信息表）
    occ.user_id AS user_id,  -- 用户ID
    occ.behavior_type AS behavior_type,  -- 行为类型（1-收藏，2-加购）
    CASE occ.behavior_type WHEN 1 THEN '收藏' WHEN 2 THEN '加购' END AS behavior_type_name,  -- 行为类型名称
    occ.behavior_time AS behavior_time,  -- 行为时间
    to_date(occ.behavior_time) AS behavior_date,  -- 行为日期
    occ.add_cart_quantity AS add_cart_quantity,  -- 加购件数
    current_timestamp() AS etl_time  -- ETL处理时间
FROM ods_commodity_collect_cart occ
-- 关联商品基础信息表获取商品名称和类目ID
         LEFT JOIN ods_commodity_base_info obi
                   ON occ.commodity_id = obi.commodity_id;

select * from dwd_commodity_collect_cart_detail;
-- todo 商品交易明细事实表
CREATE TABLE IF NOT EXISTS dwd_commodity_transaction_detail
(
    id
    BIGINT
    COMMENT
    '记录唯一标识',
    order_id
    BIGINT
    COMMENT
    '订单ID',
    commodity_id
    BIGINT
    COMMENT
    '商品ID',
    commodity_name
    STRING
    COMMENT
    '商品名称',
    category_id
    BIGINT
    COMMENT
    '类目ID',
    category_name
    STRING
    COMMENT
    '类目名称',
    user_id
    BIGINT
    COMMENT
    '用户ID',
    is_new_user
    TINYINT
    COMMENT
    '是否新用户（0-否，1-是）',
    terminal_type
    TINYINT
    COMMENT
    '终端类型（1-PC端，2-无线端）',
    order_time
    TIMESTAMP
    COMMENT
    '下单时间',
    order_date
    DATE
    COMMENT
    '下单日期',
    pay_time
    TIMESTAMP
    COMMENT
    '支付时间',
    pay_date
    DATE
    COMMENT
    '支付日期',
    quantity
    INT
    COMMENT
    '件数',
    amount
    DECIMAL
(
    10,
    2
) COMMENT '金额',
    order_status TINYINT COMMENT '订单状态',
    order_status_name STRING COMMENT '订单状态名称',
    is_pre_sale TINYINT COMMENT '是否预售',
    refund_amount DECIMAL
(
    10,
    2
) COMMENT '退款金额',
    is_juhuasuan TINYINT COMMENT '是否聚划算活动',
    etl_time TIMESTAMP COMMENT 'ETL处理时间'
    ) COMMENT '商品交易明细事实表'
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\t'
    STORED AS ORC;

-- 生成商品交易明细事实表数据
INSERT INTO TABLE dwd_commodity_transaction_detail
SELECT
    -- 交易记录唯一标识
    oct.id AS id,
    -- 订单ID
    oct.order_id AS order_id,
    -- 商品ID
    oct.commodity_id AS commodity_id,
    -- 商品名称（关联商品基础信息表）
    obi.commodity_name AS commodity_name,
    -- 类目ID（关联商品基础信息表）
    obi.category_id AS category_id,
    -- 类目名称（关联类目信息表）
    ci.category_name AS category_name,
    -- 用户ID
    oct.user_id AS user_id,
    -- 是否新用户（基于首次支付时间判断：无首次支付记录则为新用户）
    CASE
        WHEN ui.first_pay_time IS NULL THEN 1
        ELSE 0
        END AS is_new_user,
    -- 终端类型
    oct.terminal_type AS terminal_type,
    -- 下单时间
    oct.order_time AS order_time,
    -- 下单日期
    to_date(oct.order_time) AS order_date,
    -- 支付时间
    oct.pay_time AS pay_time,
    -- 支付日期
    to_date(oct.pay_time) AS pay_date,
    -- 件数
    oct.quantity AS quantity,
    -- 金额
    oct.amount AS amount,
    -- 订单状态
    oct.order_status AS order_status,
    -- 订单状态名称（转换编码为名称）
    CASE oct.order_status
        WHEN 1 THEN '已下单未支付'
        WHEN 2 THEN '已支付'
        WHEN 3 THEN '已取消'
        END AS order_status_name,
    -- 是否预售
    oct.is_pre_sale AS is_pre_sale,
    -- 退款金额
    oct.refund_amount AS refund_amount,
    -- 是否聚划算活动
    oct.is_juhuasuan AS is_juhuasuan,
    -- ETL处理时间
    current_timestamp() AS etl_time
-- 关联ODS层表
FROM ods_commodity_transaction oct
-- 关联商品基础信息表获取商品名称和类目ID
         LEFT JOIN ods_commodity_base_info obi
                   ON oct.commodity_id = obi.commodity_id
-- 关联类目信息表获取类目名称
         LEFT JOIN ods_category_info ci
                   ON obi.category_id = ci.category_id
-- 关联用户信息表判断是否新用户
         LEFT JOIN ods_user_info ui
                   ON oct.user_id = ui.user_id;

select * from dwd_commodity_transaction_detail;

-- todo 1111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111
-- todo 商品基础维度表
CREATE TABLE IF NOT EXISTS dwd_dim_commodity_base
(
    commodity_id
    BIGINT
    COMMENT
    '商品唯一标识，主键',
    commodity_name
    STRING
    COMMENT
    '商品名称',
    category_id
    BIGINT
    COMMENT
    '类目ID',
    category_name
    STRING
    COMMENT
    '类目名称',
    parent_category_id
    BIGINT
    COMMENT
    '父类目ID',
    parent_category_name
    STRING
    COMMENT
    '父类目名称',
    price
    DECIMAL
(
    10,
    2
) COMMENT '商品价格',
    create_time TIMESTAMP COMMENT '商品创建时间',
    update_time TIMESTAMP COMMENT '商品更新时间',
    etl_time TIMESTAMP COMMENT 'ETL处理时间'
    ) COMMENT '商品基础维度表'
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\t'
    STORED AS ORC;

-- 分批次生成商品基础维度表数据，单批次200条
INSERT INTO TABLE dwd_dim_commodity_base
SELECT
    obi.commodity_id AS commodity_id,
    obi.commodity_name AS commodity_name,
    obi.category_id AS category_id,
    ci.category_name AS category_name,
    ci.parent_id AS parent_category_id,
    -- 处理父类目名称可能为NULL的情况（一级类目无父类目）
    CASE WHEN pc.category_name IS NULL THEN '无父类目' ELSE pc.category_name END AS parent_category_name,
    obi.price AS price,
    obi.create_time AS create_time,
    obi.update_time AS update_time,
    current_timestamp() AS etl_time
FROM (
         -- 限制单批次商品基础信息数据量
         SELECT * FROM ods_commodity_base_info LIMIT 200
     ) obi
         LEFT JOIN ods_category_info ci
                   ON obi.category_id = ci.category_id
         LEFT JOIN ods_category_info pc
                   ON ci.parent_id = pc.category_id;

select * from dwd_dim_commodity_base;
-- todo 类目维度表
CREATE TABLE IF NOT EXISTS dwd_dim_category
(
    category_id
    BIGINT
    COMMENT
    '类目唯一标识，主键',
    category_name
    STRING
    COMMENT
    '类目名称',
    parent_id
    BIGINT
    COMMENT
    '父类目ID',
    parent_name
    STRING
    COMMENT
    '父类目名称',
    level
    INT
    COMMENT
    '类目层级',
    is_leaf
    TINYINT
    COMMENT
    '是否叶子类目（0-否，1-是）',
    etl_time
    TIMESTAMP
    COMMENT
    'ETL处理时间'
) COMMENT '类目维度表'
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\t'
    STORED AS ORC;

-- 分批次生成数据，限制单批次处理量
Insert INTO TABLE dwd_dim_category
SELECT
    ci.category_id AS category_id,
    ci.category_name AS category_name,
    ci.parent_id AS parent_id,
    pc.category_name AS parent_name,
    ci.level AS level,
    -- 无下级类目则为叶子类目（1-是，0-否）
    CASE WHEN COUNT(c.child_id) = 0 THEN 1 ELSE 0 END AS is_leaf,
    current_timestamp() AS etl_time
FROM (
         -- 限制单批次处理的类目数量
         SELECT * FROM ods_category_info LIMIT 100
     ) ci
         LEFT JOIN ods_category_info pc
                   ON ci.parent_id = pc.category_id
         LEFT JOIN (
    SELECT parent_id AS child_id
    FROM ods_category_info
    WHERE parent_id IS NOT NULL
) c ON ci.category_id = c.child_id
GROUP BY ci.category_id, ci.category_name, ci.parent_id, pc.category_name, ci.level;

select * from dwd_dim_category;
-- todo 用户维度表
CREATE TABLE IF NOT EXISTS dwd_dim_user
(
    user_id
    BIGINT
    COMMENT
    '用户唯一标识，主键',
    first_pay_time
    TIMESTAMP
    COMMENT
    '首次支付时间',
    user_level
    STRING
    COMMENT
    '用户等级',
    etl_time
    TIMESTAMP
    COMMENT
    'ETL处理时间'
) COMMENT '用户维度表'
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\t'
    STORED AS ORC;

-- 生成用户维度表数据
INSERT INTO TABLE dwd_dim_user
SELECT
    ui.user_id AS user_id,  -- 用户唯一标识
    ui.first_pay_time AS first_pay_time,  -- 首次支付时间
    -- 根据首次支付时间划分用户等级
    CASE
        WHEN ui.first_pay_time IS NULL THEN '新用户'  -- 无首次支付记录
        WHEN datediff(current_date, ui.first_pay_time) < 90 THEN '铜牌用户'  -- 首次支付不满90天
        WHEN datediff(current_date, ui.first_pay_time) < 180 THEN '银牌用户'  -- 首次支付90-179天
        ELSE '金牌用户'  -- 首次支付180天及以上
        END AS user_level,
    current_timestamp() AS etl_time  -- ETL处理时间
FROM ods_user_info ui;

select * from dwd_dim_user;
