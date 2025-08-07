-- todo ODS层：存储原始数据，保持数据原始性和完整性
-- todo 创建数据库 进入数据库
CREATE DATABASE IF NOT EXISTS work_order_01_double;

USE work_order_01_double;

-- todo 商品访问行为表
CREATE TABLE IF NOT EXISTS ods_commodity_visit_behavior
(
    id
        BIGINT
        COMMENT
            '记录唯一标识，主键',
    commodity_id
        BIGINT
        COMMENT
            '商品ID',
    visitor_id
        BIGINT
        COMMENT
            '访客ID',
    visit_time
        TIMESTAMP
        COMMENT
            '访问时间',
    terminal_type
        TINYINT
        COMMENT
            '终端类型（1-PC端，2-无线端）',
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
) COMMENT '商品访问行为原始数据表'
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\t'
    STORED AS ORC;

-- 向商品访问行为表插入模拟数据，兼容低版本Hive
INSERT INTO TABLE ods_commodity_visit_behavior
SELECT
    -- 生成唯一ID（结合随机数与自增序列）
    row_number() OVER () AS id,
    -- 商品ID：匹配文档中需分析的商品范围（100-200）
    cast(floor(rand() * 100) + 100 AS BIGINT) AS commodity_id,
    -- 访客ID：模拟1000-5000区间的用户
    cast(floor(rand() * 4000) + 1000 AS BIGINT) AS visitor_id,
    -- 访问时间：近30天内的随机时间，精确到秒
    from_unixtime(
            unix_timestamp(date_add(current_date, -cast(floor(rand() * 30) AS INT)))
                + cast(floor(rand() * 86400) AS INT) -- 一天总秒数（24×3600）
    ) AS visit_time,
    -- 终端类型：1-PC端（30%），2-无线端（70%）
    cast(CASE WHEN rand() > 0.3 THEN 2 ELSE 1 END AS TINYINT) AS terminal_type,
    -- 停留时长：分层模拟（3-300秒），符合实际访问规律
    cast(CASE
             WHEN rand() > 0.8 THEN rand() * 240 + 60  -- 10%用户停留60-300秒
             WHEN rand() > 0.2 THEN rand() * 50 + 10   -- 60%用户停留10-60秒
             ELSE rand() * 7 + 3                      -- 30%用户停留3-10秒
        END AS INT) AS stay_duration,
    -- 是否点击：80%概率有点击行为
    cast(CASE WHEN rand() > 0.2 THEN 1 ELSE 0 END AS TINYINT) AS is_click,
    -- 是否浏览微详情：40%概率访问微详情
    cast(CASE WHEN rand() > 0.6 THEN 1 ELSE 0 END AS TINYINT) AS is_micro_detail,
    -- 微详情停留时长：访问微详情时为3-60秒，否则为0
    cast(CASE
             WHEN rand() > 0.6 THEN rand() * 57 + 3
             ELSE 0
        END AS INT) AS micro_detail_stay,
    -- ETL处理时间：当前时间
    current_timestamp() AS etl_time
-- 生成5000条数据（通过多层UNION ALL实现，兼容无array_repeat函数的低版本Hive）
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
) t3; -- 100×5=500条（如需更多数据，可增加t4表继续关联）

select *
from ods_commodity_visit_behavior;

-- todo 商品收藏加购表
CREATE TABLE IF NOT EXISTS ods_commodity_collect_cart
(
    id
        BIGINT
        COMMENT
            '记录唯一标识，主键',
    commodity_id
        BIGINT
        COMMENT
            '商品ID',
    user_id
        BIGINT
        COMMENT
            '用户ID',
    behavior_type
        TINYINT
        COMMENT
            '行为类型（1-收藏，2-加购）',
    behavior_time
        TIMESTAMP
        COMMENT
            '行为时间',
    add_cart_quantity
        INT
        COMMENT
            '加购件数（行为类型为加购时有效）',
    etl_time
        TIMESTAMP
        COMMENT
            'ETL处理时间'
) COMMENT '商品收藏加购原始数据表'
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\t'
    STORED AS ORC;

-- 生成商品收藏加购模拟数据
INSERT INTO TABLE ods_commodity_collect_cart
SELECT
    -- 生成唯一记录ID
    row_number() OVER ()                        AS id,
    -- 商品ID：关联100-200区间的商品（与访问行为表商品范围一致）
    cast(floor(rand() * 100) + 100 AS BIGINT)   AS commodity_id,
    -- 用户ID：模拟5000-10000区间的用户
    cast(floor(rand() * 5000) + 5000 AS BIGINT) AS user_id,
    -- 行为类型：1-收藏，2-加购（加购行为占比略高）
    CASE WHEN rand() > 0.4 THEN 2 ELSE 1 END    AS behavior_type,
    -- 行为时间：近30天内的随机时间，与访问行为时间范围匹配
    from_unixtime(
            unix_timestamp(date_add(current_date, -cast(floor(rand() * 30) AS INT)))
                + cast(floor(rand() * 24 * 3600) AS INT) -- 随机小时（转换为秒）
                + cast(floor(rand() * 60) AS INT) -- 随机分钟
                + cast(floor(rand() * 60) AS INT) -- 随机秒
    )                                           AS behavior_time,
    -- 加购件数：行为类型为加购时生成1-5件，收藏时为0
    CASE
        WHEN rand() > 0.4 THEN cast(floor(rand() * 5) + 1 AS INT) -- 加购时1-5件
        ELSE 0 -- 收藏时无效，默认为0
        END                                     AS add_cart_quantity,
    -- ETL处理时间：当前时间
    current_timestamp()                         AS etl_time
-- 分批次生成5万条数据（通过多表关联实现，兼容低版本Hive）
FROM (SELECT 1 AS num
      UNION ALL
      SELECT 1
      UNION ALL
      SELECT 1
      UNION ALL
      SELECT 1
      UNION ALL
      SELECT 1
      UNION ALL
      SELECT 1
      UNION ALL
      SELECT 1
      UNION ALL
      SELECT 1
      UNION ALL
      SELECT 1
      UNION ALL
      SELECT 1) t1
         CROSS JOIN (SELECT 1 AS num
                     UNION ALL
                     SELECT 1
                     UNION ALL
                     SELECT 1
                     UNION ALL
                     SELECT 1
                     UNION ALL
                     SELECT 1
                     UNION ALL
                     SELECT 1
                     UNION ALL
                     SELECT 1
                     UNION ALL
                     SELECT 1
                     UNION ALL
                     SELECT 1
                     UNION ALL
                     SELECT 1) t2
         CROSS JOIN (SELECT 1 AS num
                     UNION ALL
                     SELECT 1
                     UNION ALL
                     SELECT 1
                     UNION ALL
                     SELECT 1
                     UNION ALL
                     SELECT 1
                     UNION ALL
                     SELECT 1
                     UNION ALL
                     SELECT 1
                     UNION ALL
                     SELECT 1
                     UNION ALL
                     SELECT 1
                     UNION ALL
                     SELECT 1) t3
         CROSS JOIN (SELECT 1 AS num
                     UNION ALL
                     SELECT 1
                     UNION ALL
                     SELECT 1
                     UNION ALL
                     SELECT 1
                     UNION ALL
                     SELECT 1 -- 控制总条数为10×10×10×5=50000
) t4;

select *
from ods_commodity_collect_cart;

-- todo 商品交易表
CREATE TABLE IF NOT EXISTS ods_commodity_transaction
(
    id
                  BIGINT
        COMMENT
            '记录唯一标识，主键',
    order_id
                  BIGINT
        COMMENT
            '订单ID',
    commodity_id
                  BIGINT
        COMMENT
            '商品ID',
    user_id
                  BIGINT
        COMMENT
            '用户ID',
    terminal_type
                  TINYINT
        COMMENT
            '终端类型（1-PC端，2-无线端）',
    order_time
                  TIMESTAMP
        COMMENT
            '下单时间',
    pay_time
                  TIMESTAMP
        COMMENT
            '支付时间',
    quantity
                  INT
        COMMENT
            '下单/支付件数',
    amount
                  DECIMAL(10,
                      2) COMMENT '下单/支付金额',
    order_status  TINYINT COMMENT '订单状态（1-已下单未支付，2-已支付，3-已取消）',
    is_pre_sale   TINYINT COMMENT '是否预售（0-否，1-是）',
    refund_amount DECIMAL(10,
                      2) COMMENT '退款金额',
    is_juhuasuan  TINYINT COMMENT '是否聚划算活动（0-否，1-是）',
    etl_time      TIMESTAMP COMMENT 'ETL处理时间'
) COMMENT '商品交易原始数据表'
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\t'
    STORED AS ORC;

-- 生成商品交易模拟数据
INSERT INTO TABLE ods_commodity_transaction
SELECT
    -- 唯一记录ID
    row_number() OVER ()                                                                     AS id,
    -- 订单ID：模拟10000-50000区间唯一订单
    cast(floor(rand() * 40000) + 10000 AS BIGINT)                                            AS order_id,
    -- 商品ID：关联100-200区间（与访问、收藏表一致）
    cast(floor(rand() * 100) + 100 AS BIGINT)                                                AS commodity_id,
    -- 用户ID：模拟5000-10000区间用户
    cast(floor(rand() * 5000) + 5000 AS BIGINT)                                              AS user_id,
    -- 终端类型：1-PC端，2-无线端（无线端占比更高）
    CASE WHEN rand() > 0.3 THEN 2 ELSE 1 END                                                 AS terminal_type,
    -- 下单时间：近30天内随机时间
    from_unixtime(
            unix_timestamp(date_add(current_date, -cast(floor(rand() * 30) AS INT)))
                + cast(floor(rand() * 24 * 3600) AS INT)
                + cast(floor(rand() * 60) AS INT)
                + cast(floor(rand() * 60) AS INT)
    )                                                                                        AS order_time,
    -- 支付时间：已支付订单在下单后0-24小时，未支付则为NULL
    from_unixtime(
            unix_timestamp(
                    from_unixtime(
                            unix_timestamp(date_add(current_date, -cast(floor(rand() * 30) AS INT)))
                                + cast(floor(rand() * 24 * 3600) AS INT)
                                + cast(floor(rand() * 60) AS INT)
                                + cast(floor(rand() * 60) AS INT)
                    )
            ) + CASE
                    WHEN rand() > 0.3 THEN cast(floor(rand() * 24 * 3600) AS INT)
                    ELSE 0
                END
    )                                                                                        AS pay_time,
    -- 下单件数：1-10件（符合支付件数统计逻辑）
    cast(floor(rand() * 10) + 1 AS INT)                                                      AS quantity,
    -- 下单金额：单价80-500元×件数
    cast((floor(rand() * 420) + 80) * cast(floor(rand() * 10) + 1 AS INT) AS DECIMAL(10, 2)) AS amount,
    -- 订单状态：1-未支付（20%），2-已支付（70%），3-已取消（10%）
    CASE
        WHEN rand() > 0.3 THEN 2
        WHEN rand() > 0.1 THEN 1
        ELSE 3
        END                                                                                  AS order_status,
    -- 是否预售：0-否（90%），1-是（10%）
    CASE WHEN rand() > 0.9 THEN 1 ELSE 0 END                                                 AS is_pre_sale,
    -- 退款金额：已支付订单中10%产生退款
    CASE
        WHEN rand() > 0.3 AND rand() > 0.9
            THEN cast(
                (floor(rand() * 420) + 80) * cast(floor(rand() * 10) + 1 AS INT) * (rand() * 0.9 + 0.1)
            AS DECIMAL(10, 2)
                 )
        ELSE 0.00
        END                                                                                  AS refund_amount,
    -- 是否聚划算活动：0-否（85%），1-是（15%）
    CASE WHEN rand() > 0.85 THEN 1 ELSE 0 END                                                AS is_juhuasuan,
    -- ETL处理时间
    current_timestamp()                                                                      AS etl_time
-- 生成1000条数据（通过多表关联控制数量）
FROM (SELECT 1 AS num
      UNION ALL
      SELECT 1
      UNION ALL
      SELECT 1
      UNION ALL
      SELECT 1
      UNION ALL
      SELECT 1
      UNION ALL
      SELECT 1
      UNION ALL
      SELECT 1
      UNION ALL
      SELECT 1
      UNION ALL
      SELECT 1
      UNION ALL
      SELECT 1) t1
         CROSS JOIN (SELECT 1 AS num
                     UNION ALL
                     SELECT 1
                     UNION ALL
                     SELECT 1
                     UNION ALL
                     SELECT 1
                     UNION ALL
                     SELECT 1
                     UNION ALL
                     SELECT 1
                     UNION ALL
                     SELECT 1
                     UNION ALL
                     SELECT 1
                     UNION ALL
                     SELECT 1
                     UNION ALL
                     SELECT 1) t2
LIMIT 1000;

select *
from ods_commodity_transaction;
-- todo 商品基础信息表
CREATE TABLE IF NOT EXISTS ods_commodity_base_info
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
            '商品所属类目ID',
    price
                DECIMAL(10,
                    2) COMMENT '商品价格',
    create_time TIMESTAMP COMMENT '商品创建时间',
    update_time TIMESTAMP COMMENT '商品更新时间',
    etl_time    TIMESTAMP COMMENT 'ETL处理时间'
) COMMENT '商品基础信息原始数据表'
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\t'
    STORED AS ORC;

-- 生成商品基础信息模拟数据
INSERT INTO TABLE ods_commodity_base_info
SELECT
    -- 商品唯一标识（100-500区间，与交易、访问表关联）
    cast(floor(rand() * 400) + 100 AS BIGINT)                 AS commodity_id,
    -- 商品名称（模拟分类命名，包含类目特征）
    concat(
            CASE floor(rand() * 5)
                WHEN 0 THEN '男装-'
                WHEN 1 THEN '女装-'
                WHEN 2 THEN '数码-'
                WHEN 3 THEN '家居-'
                ELSE '美妆-'
                END,
            '款号',
            cast(floor(rand() * 1000) + 100 AS STRING)
    )                                                         AS commodity_name,
    -- 类目ID（10-50区间，模拟叶子类目）
    cast(floor(rand() * 40) + 10 AS BIGINT)                   AS category_id,
    -- 商品价格（50-1000元，支持价格带区间分析）
    cast(floor(rand() * 950) + 50 AS DECIMAL(10, 2))          AS price,
    -- 创建时间（近1年内随机时间）
    date_add(current_date, -cast(floor(rand() * 365) AS INT)) AS create_time,
    -- 更新时间（创建时间之后的随机时间，修正时间计算逻辑）
    date_add(
            date_add(current_date, -cast(floor(rand() * 365) AS INT)),
            cast(floor(rand() * 30) AS INT) -- 创建后0-30天内更新（按天数计算）
    )                                                         AS update_time,
    -- ETL处理时间
    current_timestamp()                                       AS etl_time
-- 生成500条商品基础数据（覆盖100-500的商品ID范围）
FROM (SELECT 1 AS num
      UNION ALL
      SELECT 1
      UNION ALL
      SELECT 1
      UNION ALL
      SELECT 1
      UNION ALL
      SELECT 1
      UNION ALL
      SELECT 1
      UNION ALL
      SELECT 1
      UNION ALL
      SELECT 1
      UNION ALL
      SELECT 1
      UNION ALL
      SELECT 1) t1
         CROSS JOIN (SELECT 1 AS num
                     UNION ALL
                     SELECT 1
                     UNION ALL
                     SELECT 1
                     UNION ALL
                     SELECT 1
                     UNION ALL
                     SELECT 1
                     UNION ALL
                     SELECT 1
                     UNION ALL
                     SELECT 1
                     UNION ALL
                     SELECT 1
                     UNION ALL
                     SELECT 1
                     UNION ALL
                     SELECT 1) t2
         CROSS JOIN (SELECT 1 AS num
                     UNION ALL
                     SELECT 1
                     UNION ALL
                     SELECT 1
                     UNION ALL
                     SELECT 1
                     UNION ALL
                     SELECT 1 -- 总条数=10×10×5=500
) t3;

select *
from ods_commodity_base_info;
-- todo 类目信息表
CREATE TABLE IF NOT EXISTS ods_category_info
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
    level
        INT
        COMMENT
            '类目层级',
    etl_time
        TIMESTAMP
        COMMENT
            'ETL处理时间'
) COMMENT '类目信息原始数据表'
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\t'
    STORED AS ORC;

-- 生成类目信息模拟数据
INSERT INTO TABLE ods_category_info
SELECT
    -- 类目唯一标识（1-50区间，包含多级类目）
    category_id,
    -- 类目名称（按层级区分）
    CASE
        WHEN level = 1 THEN concat('一级类目-', category_name_suffix)
        WHEN level = 2 THEN concat('二级类目-', category_name_suffix)
        WHEN level = 3 THEN concat('三级类目-', category_name_suffix)
        END             AS category_name,
    -- 父类目ID（一级类目父ID为0，多级类目关联上级）
    parent_id,
    -- 类目层级（1-3级）
    level,
    -- ETL处理时间
    current_timestamp() AS etl_time
FROM (
         -- 一级类目（1-10）
         SELECT cast(floor(rand() * 10) + 1 AS BIGINT) AS category_id,
                0                                      AS parent_id,
                1                                      AS level,
                CASE floor(rand() * 5)
                    WHEN 0 THEN '男装'
                    WHEN 1 THEN '女装'
                    WHEN 2 THEN '数码'
                    WHEN 3 THEN '家居'
                    ELSE '美妆'
                    END                                AS category_name_suffix
         FROM (SELECT 1 AS num UNION ALL SELECT 1) t
         UNION ALL
         -- 二级类目（11-30，父ID关联一级类目）
         SELECT cast(floor(rand() * 20) + 11 AS BIGINT) AS category_id,
                cast(floor(rand() * 10) + 1 AS BIGINT)  AS parent_id,
                2                                       AS level,
                CASE floor(rand() * 3)
                    WHEN 0 THEN '上衣'
                    WHEN 1 THEN '裤子'
                    ELSE '配饰'
                    END                                 AS category_name_suffix
         FROM (SELECT 1 AS num UNION ALL SELECT 1 UNION ALL SELECT 1) t
         UNION ALL
         -- 三级类目（31-50，父ID关联二级类目）
         SELECT cast(floor(rand() * 20) + 31 AS BIGINT) AS category_id,
                cast(floor(rand() * 20) + 11 AS BIGINT) AS parent_id,
                3                                       AS level,
                CASE floor(rand() * 4)
                    WHEN 0 THEN 'T恤'
                    WHEN 1 THEN '衬衫'
                    WHEN 2 THEN '牛仔裤'
                    ELSE '卫衣'
                    END                                 AS category_name_suffix
         FROM (SELECT 1 AS num UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1) t) t;

select *
from ods_category_info;
-- todo 用户信息表
CREATE TABLE IF NOT EXISTS ods_user_info
(
    user_id
        BIGINT
        COMMENT
            '用户唯一标识，主键',
    first_pay_time
        TIMESTAMP
        COMMENT
            '首次支付时间',
    etl_time
        TIMESTAMP
        COMMENT
            'ETL处理时间'
) COMMENT '用户信息原始数据表'
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\t'
    STORED AS ORC;

-- 分批次生成数据，单批次1000条，避免任务压力过大
INSERT INTO TABLE ods_user_info
SELECT
    -- 用户唯一标识（5000-10000区间，与交易、收藏表关联）
    cast(floor(rand() * 5000) + 5000 AS BIGINT) AS user_id,
    -- 首次支付时间：区分新老用户（符合文档中支付新/老买家数定义）
    CASE
        WHEN rand() > 0.3 THEN date_add(current_date, -cast(floor(rand() * 365) AS INT))
        ELSE NULL -- 30%用户为新用户（无首次支付记录）
        END                                     AS first_pay_time,
    -- ETL处理时间
    current_timestamp()                         AS etl_time
-- 生成1000条数据（精简关联表数量，降低执行复杂度）
FROM (SELECT 1 AS num
      UNION ALL
      SELECT 1
      UNION ALL
      SELECT 1
      UNION ALL
      SELECT 1
      UNION ALL
      SELECT 1
      UNION ALL
      SELECT 1
      UNION ALL
      SELECT 1
      UNION ALL
      SELECT 1
      UNION ALL
      SELECT 1
      UNION ALL
      SELECT 1) t1
         CROSS JOIN (SELECT 1 AS num
                     UNION ALL
                     SELECT 1
                     UNION ALL
                     SELECT 1
                     UNION ALL
                     SELECT 1
                     UNION ALL
                     SELECT 1
                     UNION ALL
                     SELECT 1
                     UNION ALL
                     SELECT 1
                     UNION ALL
                     SELECT 1
                     UNION ALL
                     SELECT 1
                     UNION ALL
                     SELECT 1) t2
LIMIT 1000; -- 限制单批次数据量

select *
from ods_user_info;