USE rewrite_work_order_03;

-- TODO 1. 商品基础信息表（ods_product_info）
DROP TABLE IF EXISTS ods_product_info;
CREATE TABLE IF NOT EXISTS ods_product_info
(
    product_id   STRING COMMENT '商品ID',
    product_name STRING COMMENT '商品名称',
    category_id  STRING COMMENT '类目ID',
    brand_id     STRING COMMENT '品牌ID',
    create_time  STRING COMMENT '创建时间（yyyy-MM-dd HH:mm:ss）',
    update_time  STRING COMMENT '更新时间（yyyy-MM-dd HH:mm:ss）',
    is_delete    STRING COMMENT '是否删除（0-否，1-是）'
) COMMENT '商品基础信息原始数据'
    PARTITIONED BY (dt STRING COMMENT '分区日期（yyyy-MM-dd）')
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE;

-- 生成100条模拟数据（可通过调整数组长度控制数量）
INSERT INTO TABLE ods_product_info PARTITION (dt='2025-01-26')
SELECT
    product_id,
    product_name,
    category_id,
    brand_id,
    create_time,
    -- 更新时间：基于子查询生成的create_time计算
    concat(
            '2025-01-',
        -- 确保日期不超过1月31日（若超31则取31）
            lpad(least(cast(substr(create_time, 9, 2) as int) + floor(rand() * 20) + 1, 31), 2, '0'),
            ' ',
            lpad(floor(rand() * 24), 2, '0'), ':',
            lpad(floor(rand() * 60), 2, '0'), ':',
            lpad(floor(rand() * 60), 2, '0')
    ) AS update_time,
    is_delete
FROM (
         SELECT
             -- 商品ID：P+随机5位数字
             concat('P', lpad(floor(rand() * 100000), 5, '0')) AS product_id,
             -- 商品名称：随机拼接品类+特征
             concat(
                     case floor(rand() * 5)
                         when 0 then '无线'
                         when 1 then '智能'
                         when 2 then '纯棉'
                         when 3 then '北欧风'
                         else '机械'
                         end,
                     case floor(rand() * 6)
                         when 0 then '耳机'
                         when 1 then '手表'
                         when 2 then 'T恤'
                         when 3 then '台灯'
                         when 4 then '键盘'
                         else '枕头'
                         end,
                     case floor(rand() * 3)
                         when 0 then 'Pro'
                         when 1 then 'Ultra'
                         else ''
                         end
             ) AS product_name,
             -- 类目ID：C001-C005随机
             concat('C00', floor(rand() * 5) + 1) AS category_id,
             -- 品牌ID：B001-B008随机
             concat('B00', floor(rand() * 8) + 1) AS brand_id,
             -- 创建时间：2025-01-01至2025-01-10随机
             concat(
                     '2025-01-',
                     lpad(floor(rand() * 10) + 1, 2, '0'),
                     ' ',
                     lpad(floor(rand() * 24), 2, '0'), ':',
                     lpad(floor(rand() * 60), 2, '0'), ':',
                     lpad(floor(rand() * 60), 2, '0')
             ) AS create_time,
             -- 是否删除：90%概率未删除（0），10%概率已删除（1）
             case when rand() < 0.9 then '0' else '1' end AS is_delete
         FROM
             (SELECT explode(array(1,2,3,4,5,6,7,8,9,10)) AS num) t1
                 LATERAL VIEW
                 explode(array(1,2,3,4,5,6,7,8,9,10)) t2 AS num2 -- 交叉连接生成100条数据
     ) t; -- 子查询别名t，用于外部引用create_time

select * from ods_product_info;
-- TODO 2. SKU 信息表（ods_sku_info）
DROP TABLE IF EXISTS ods_sku_info;
CREATE TABLE IF NOT EXISTS ods_sku_info
(
    sku_id        STRING COMMENT 'SKU ID',
    product_id    STRING COMMENT '商品ID',
    sku_attr_json STRING COMMENT 'SKU属性（JSON格式，如{"颜色":"红色","尺寸":"XL"}）',
    price         DECIMAL(10, 2) COMMENT '售价',
    stock         INT COMMENT '库存数量',
    create_time   STRING COMMENT '创建时间（yyyy-MM-dd HH:mm:ss）',
    update_time   STRING COMMENT '更新时间（yyyy-MM-dd HH:mm:ss）'
) COMMENT 'SKU原始数据'
    PARTITIONED BY (dt STRING COMMENT '分区日期（yyyy-MM-dd）')
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE;

-- 生成与商品表关联的SKU模拟数据（每个商品随机生成1-3个SKU）
INSERT INTO TABLE ods_sku_info PARTITION (dt='2025-01-26')
SELECT
    -- SKU ID：S+商品ID后5位+随机2位数字（确保唯一性）
    concat('S', substr(t1.product_id, 2), lpad(floor(rand() * 100), 2, '0')) AS sku_id,
    t1.product_id AS product_id,
    -- SKU属性JSON：根据商品名称动态生成合理属性
    case
        when t1.product_name like '%耳机%' or t1.product_name like '%手表%' or t1.product_name like '%键盘%'
            then concat('{"版本":"', case floor(rand() * 3)
                                         when 0 then '标准版'
                                         when 1 then '高配版'
                                         else '限量版'
            end, '","颜色":"', case floor(rand() * 4)
                                   when 0 then '白色'
                                   when 1 then '黑色'
                                   when 2 then '银色'
                                   else '蓝色'
                            end, '"}')
        when t1.product_name like '%T恤%' or t1.product_name like '%衬衫%' or t1.product_name like '%卫衣%'
            then concat('{"颜色":"', case floor(rand() * 5)
                                         when 0 then '红色'
                                         when 1 then '黑色'
                                         when 2 then '白色'
                                         when 3 then '蓝色'
                                         else '灰色'
            end, '","尺寸":"', case floor(rand() * 5)
                                   when 0 then 'S'
                                   when 1 then 'M'
                                   when 2 then 'L'
                                   when 3 then 'XL'
                                   else 'XXL'
                            end, '"}')
        else concat('{"款式":"', case floor(rand() * 2)
                                     when 0 then '基础款'
                                     else '升级款'
            end, '"}')
        end AS sku_attr_json,
    -- 价格：基于类目动态定价
    case
        when t1.category_id in ('C001', 'C002') then 300 + floor(rand() * 700) + rand() -- 电子类
        when t1.category_id = 'C003' then 50 + floor(rand() * 250) + rand() -- 服装类
        else 100 + floor(rand() * 400) + rand() -- 家居类
        end AS price,
    -- 库存：50-200之间随机
    50 + floor(rand() * 150) AS stock,
    -- 创建时间：与商品创建时间相同或延后1天内
    concat(
            substr(t1.create_time, 1, 10), ' ',
            lpad(floor(rand() * 24), 2, '0'), ':',
            lpad(floor(rand() * 60), 2, '0'), ':',
            lpad(floor(rand() * 60), 2, '0')
    ) AS create_time,
    -- 更新时间：创建时间之后随机1-10天（关键修正：转换为INT类型）
    concat(
            date_add(substr(t1.create_time, 1, 10), cast(floor(rand() * 10) + 1 AS INT)), ' ',
            lpad(floor(rand() * 24), 2, '0'), ':',
            lpad(floor(rand() * 60), 2, '0'), ':',
            lpad(floor(rand() * 60), 2, '0')
    ) AS update_time
FROM
    ods_product_info t1
        LATERAL VIEW
        -- 每个商品随机生成1-3个SKU
        explode(split(space(cast(floor(rand() * 3) + 1 AS INT)), ' ')) t2 AS sku_num
WHERE
    t1.dt = '2025-01-26'
  AND t1.is_delete = '0';

select * from ods_sku_info;
-- TODO 3. 用户行为日志表（ods_user_behavior_log）
DROP TABLE IF EXISTS ods_user_behavior_log;
CREATE TABLE IF NOT EXISTS ods_user_behavior_log
(
    log_id        STRING COMMENT '日志唯一ID',
    user_id       STRING COMMENT '用户ID',
    product_id    STRING COMMENT '商品ID',
    sku_id        STRING COMMENT 'SKU ID',
    behavior_type STRING COMMENT '行为类型（click-点击、view-浏览、pay-支付、comment-评价、search-搜索）',
    behavior_time STRING COMMENT '行为时间（yyyy-MM-dd HH:mm:ss）',
    channel       STRING COMMENT '行为渠道（如手淘搜索、直通车、直播）',
    keyword       STRING COMMENT '搜索关键词（行为类型为search时有效）',
    content_id    STRING COMMENT '内容ID（如直播/短视频ID，从内容跳转时有效）',
    stay_time     INT COMMENT '停留时间（秒，浏览行为时有效）'
) COMMENT '用户行为原始日志'
    PARTITIONED BY (dt STRING COMMENT '分区日期（yyyy-MM-dd）')
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE;

-- 生成用户行为日志模拟数据（修正版，解决变量引用错误）
INSERT INTO TABLE ods_user_behavior_log PARTITION (dt='2025-01-26')
SELECT
    -- 日志ID：L+日期+随机8位数字（确保唯一性）
    concat('L', regexp_replace('2025-01-26', '-', ''), lpad(floor(rand() * 100000000), 8, '0')) AS log_id,
    -- 用户ID：U+随机6位数字（模拟10万级用户量）
    concat('U', lpad(floor(rand() * 1000000), 6, '0')) AS user_id,
    -- 商品ID：关联商品表的有效商品（未删除）
    product_id,
    -- SKU ID：与商品ID关联生成（S+商品ID后5位+随机2位）
    concat('S', substr(product_id, 2), lpad(floor(rand() * 100), 2, '0')) AS sku_id,
    -- 行为类型：按业务比例分布
    behavior_type,
    -- 行为时间：2025-01-26当天按时间段分布（高峰时段集中）
    case
        when hour_rand < 0.3 then concat('2025-01-26 ', lpad(9 + floor(rand() * 3), 2, '0'), ':', lpad(floor(rand() * 60), 2, '0'), ':', lpad(floor(rand() * 60), 2, '0'))
        when hour_rand < 0.5 then concat('2025-01-26 ', lpad(14 + floor(rand() * 4), 2, '0'), ':', lpad(floor(rand() * 60), 2, '0'), ':', lpad(floor(rand() * 60), 2, '0'))
        else concat('2025-01-26 ', lpad(20 + floor(rand() * 2), 2, '0'), ':', lpad(floor(rand() * 60), 2, '0'), ':', lpad(floor(rand() * 60), 2, '0'))
        end AS behavior_time,
    -- 行为渠道：按流量占比分布
    channel,
    -- 搜索关键词：仅search行为有值，与商品品类匹配
    case when behavior_type = 'search' then keyword else '' end AS keyword,
    -- 内容ID：仅直播/短视频渠道有值
    case when channel in ('直播', '短视频') then concat('C', lpad(floor(rand() * 100000), 5, '0')) else '' end AS content_id,
    -- 停留时间：仅view行为有值（内容渠道停留更久）
    case
        when behavior_type = 'view' then
            case when channel in ('直播', '短视频') then 60 + floor(rand() * 240)
                 else 10 + floor(rand() * 290)
                end
        else 0
        end AS stay_time
FROM (
         -- 基础数据层：生成行为类型、渠道、关键词等基础字段
         SELECT
             t1.product_id,
             -- 行为类型分布：点击(40%)、浏览(30%)、支付(10%)、评价(5%)、搜索(15%)
             case
                 when floor(rand() * 100) between 0 and 39 then 'click'
                 when floor(rand() * 100) between 40 and 69 then 'view'
                 when floor(rand() * 100) between 70 and 79 then 'pay'
                 when floor(rand() * 100) between 80 and 84 then 'comment'
                 else 'search'
                 end AS behavior_type,
             -- 渠道分布：手淘搜索(30%)、直通车(20%)、直播(15%)、短视频(15%)、首页推荐(20%)
             case
                 when floor(rand() * 100) between 0 and 29 then '手淘搜索'
                 when floor(rand() * 100) between 30 and 49 then '直通车'
                 when floor(rand() * 100) between 50 and 64 then '直播'
                 when floor(rand() * 100) between 65 and 79 then '短视频'
                 else '首页推荐'
                 end AS channel,
             -- 搜索关键词池：与商品品类匹配
             case floor(rand() * 12)
                 when 0 then '无线蓝牙耳机'
                 when 1 then '智能手表 运动'
                 when 2 then '纯棉T恤 男'
                 when 3 then '机械键盘 青轴'
                 when 4 then '北欧风台灯 卧室'
                 when 5 then '记忆棉枕头 颈椎'
                 when 6 then '修身牛仔裤 女'
                 when 7 then '连帽卫衣 加绒'
                 when 8 then '游戏鼠标 有线'
                 when 9 then '家居好物 推荐'
                 when 10 then '新年礼物 实用'
                 else '春季新款 2025'
                 end AS keyword,
             -- 用于时间分布的随机值
             rand() AS hour_rand
         FROM
             -- 从商品表获取有效商品（未删除）
             (SELECT product_id FROM ods_product_info WHERE dt='2025-01-26' AND is_delete='0') t1
                 -- 每个商品生成5-10条行为记录
                 LATERAL VIEW
                 explode(split(space(cast(floor(rand() * 6) + 5 AS INT)), ' ')) t2 AS log_num
     ) t;

select * from ods_user_behavior_log;
-- TODO 4. 订单表（ods_order_info）
DROP TABLE IF EXISTS ods_order_info;
CREATE TABLE IF NOT EXISTS ods_order_info
(
    order_id     STRING COMMENT '订单ID',
    user_id      STRING COMMENT '用户ID',
    product_id   STRING COMMENT '商品ID',
    sku_id       STRING COMMENT 'SKU ID',
    order_time   STRING COMMENT '下单时间（yyyy-MM-dd HH:mm:ss）',
    pay_time     STRING COMMENT '支付时间（yyyy-MM-dd HH:mm:ss）',
    order_amount DECIMAL(16, 2) COMMENT '订单金额',
    order_count  INT COMMENT '购买数量',
    order_status STRING COMMENT '订单状态（待支付/已支付/已取消）'
) COMMENT '订单原始数据'
    PARTITIONED BY (dt STRING COMMENT '分区日期（yyyy-MM-dd）')
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE;

-- 生成订单原始数据（修正同层级字段引用错误）
INSERT INTO TABLE ods_order_info PARTITION (dt='2025-01-26')
SELECT
    -- 订单ID：O+日期+随机8位数字
    concat('O', regexp_replace('2025-01-26', '-', ''), lpad(floor(rand() * 100000000), 8, '0')) AS order_id,
    -- 随机用户ID（U+6位数字，与行为日志格式一致）
    concat('U', lpad(floor(rand() * 1000000), 6, '0')) AS user_id,
    product_id,
    -- SKU ID：与商品ID关联
    concat('S', substr(product_id, 2), lpad(floor(rand() * 100), 2, '0')) AS sku_id,
    order_time,  -- 引用子查询中生成的下单时间
    -- 支付时间：基于子查询的order_time计算
    case
        when order_status = '已支付' then
            concat(
                    substr(order_time, 1, 10), ' ',
                    lpad(cast(substr(order_time, 12, 2) as int) + floor(minute_diff / 60), 2, '0'), ':',
                    lpad(cast(substr(order_time, 15, 2) as int) + (minute_diff % 60), 2, '0'), ':',
                    lpad(floor(rand() * 60), 2, '0')
            )
        else ''
        end AS pay_time,
    -- 订单金额：基于商品类目定价 * 数量
    (case
         when category_id in ('C001', 'C002') then 300 + floor(rand() * 700) + rand()
         when category_id = 'C003' then 50 + floor(rand() * 250) + rand()
         else 100 + floor(rand() * 400) + rand()
        end) * order_count AS order_amount,
    order_count,
    order_status
FROM (
         -- 子查询：提前生成所有基础字段，避免同层级引用
         SELECT
             t1.product_id,
             t1.category_id,
             t4.order_num,
             t2.order_status,
             t2.order_count,
             t2.minute_diff,
             -- 下单时间：在子查询中生成，供外层引用
             concat(
                     '2025-01-26 ',
                     lpad(floor(rand() * 24), 2, '0'), ':',
                     lpad(floor(rand() * 60), 2, '0'), ':',
                     lpad(floor(rand() * 60), 2, '0')
             ) AS order_time
         FROM
             -- 商品基础信息
             (SELECT p.product_id, p.category_id
              FROM ods_product_info p
              WHERE p.dt='2025-01-26' AND p.is_delete='0') t1
                 -- 生成订单数量（3-8个/商品）
                 LATERAL VIEW
                 explode(split(space(cast(floor(rand() * 6) + 3 AS INT)), ' ')) t4 AS order_num
                 -- 生成订单属性
                 LATERAL VIEW
                 inline(array(
                         named_struct(
                                 'order_status', case
                                                     when floor(rand() * 100) between 0 and 69 then '已支付'
                                                     when floor(rand() * 100) between 70 and 89 then '待支付'
                                                     else '已取消'
                             end,
                                 'order_count', floor(rand() * 5) + 1,
                                 'minute_diff', floor(rand() * 26) + 5  -- 支付时间差（5-30分钟）
                         )
                        )) t2 AS order_status, order_count, minute_diff
     ) t;  -- 子查询别名，供外层引用所有字段

select * from ods_order_info;
-- TODO 5. 评价表（ods_comment_info）
DROP TABLE IF EXISTS ods_comment_info;
CREATE TABLE IF NOT EXISTS ods_comment_info
(
    comment_id  STRING COMMENT '评价ID',
    product_id  STRING COMMENT '商品ID',
    sku_id      STRING COMMENT 'SKU ID',
    user_id     STRING COMMENT '用户ID',
    score       INT COMMENT '评分（1-5星）',
    content     STRING COMMENT '评价内容',
    create_time STRING COMMENT '评价时间（yyyy-MM-dd HH:mm:ss）',
    is_positive INT COMMENT '是否正面评价（0-否，1-是）',
    is_active   INT COMMENT '是否主动评价（0-否，1-是）',
    user_type   STRING COMMENT '用户类型（new-新用户，old-老用户）'
) COMMENT '商品评价原始数据'
    PARTITIONED BY (dt STRING COMMENT '分区日期（yyyy-MM-dd）')
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE;

-- 生成商品评价原始数据（最终修正版，解决date_add参数类型错误）
INSERT INTO TABLE ods_comment_info PARTITION (dt='2025-01-26')
SELECT
    -- 评价ID：C+日期+随机8位数字（唯一标识）
    concat('C', regexp_replace('2025-01-26', '-', ''), lpad(floor(rand() * 100000000), 8, '0')) AS comment_id,
    -- 商品ID：来源于订单表
    t2.product_id,
    -- SKU ID：与商品ID关联
    concat('S', substr(t2.product_id, 2), lpad(floor(rand() * 100), 2, '0')) AS sku_id,
    -- 用户ID：与订单用户一致
    t2.user_id,
    -- 评分：引用子查询中生成的score
    t2.score,
    -- 评价内容：基于子查询的score生成
    case
        when t2.score >= 4 then t2.positive_content
        when t2.score = 3 then t2.neutral_content
        else t2.negative_content
        end AS content,
    -- 评价时间：支付后1-7天内（修正date_add参数类型）
    concat(
            date_add(substr(t2.pay_time, 1, 10), cast(floor(rand() * 7) + 1 AS INT)), ' ',  -- 显式转换为INT
            lpad(floor(rand() * 24), 2, '0'), ':',
            lpad(floor(rand() * 60), 2, '0'), ':',
            lpad(floor(rand() * 60), 2, '0')
    ) AS create_time,
    -- 是否正面评价
    case when t2.score >= 4 then 1 else 0 end AS is_positive,
    -- 是否主动评价（30%主动）
    case when rand() < 0.3 then 1 else 0 end AS is_active,
    -- 用户类型（30%新用户，70%老用户）
    case when rand() < 0.3 then 'new' else 'old' end AS user_type
FROM (
         -- 中间子查询：提前生成score和评价内容模板
         SELECT
             t1.product_id,
             t1.user_id,
             t1.pay_time,
             t1.product_type,
             t1.positive_content,
             t1.neutral_content,
             t1.negative_content,
             -- 评分：在子查询中生成
             case
                 when rand() < 0.7 then 4 + floor(rand() * 2)
                 when rand() < 0.9 then 3
                 else 1 + floor(rand() * 2)
                 end AS score
         FROM (
                  -- 基础数据子查询：获取订单和商品信息
                  SELECT
                      oi.product_id,
                      oi.user_id,
                      oi.pay_time,
                      -- 商品类型（用于生成匹配的评价内容）
                      case
                          when pi.category_id in ('C001', 'C002') then 'electronic'
                          when pi.category_id = 'C003' then 'clothing'
                          else 'home'
                          end AS product_type,
                      -- 评价内容模板（按商品类型分类）
                      case
                          when pi.category_id in ('C001', 'C002') then concat('商品质量很好，功能符合描述，', split('运行流畅,音质出色,续航持久,外观精致,性价比高', ',')[cast(floor(rand()*5) as int)])
                          when pi.category_id = 'C003' then concat('衣服面料舒适，', split('尺码标准,款式好看,做工精细,颜色很正,穿着合身', ',')[cast(floor(rand()*5) as int)])
                          else concat('家居用品很实用，', split('材质不错,设计合理,使用方便,性价比高,推荐购买', ',')[cast(floor(rand()*5) as int)])
                          end AS positive_content,
                      case
                          when pi.category_id in ('C001', 'C002') then concat('商品还行，', split('功能基本满足,价格适中,没预期的好,暂时没发现问题,一般般', ',')[cast(floor(rand()*5) as int)])
                          when pi.category_id = 'C003' then concat('衣服不算差，', split('尺码略大,面料一般,款式普通,颜色有差异,对得起价格', ',')[cast(floor(rand()*5) as int)])
                          else concat('家居用品还行，', split('能用但不算惊艳,设计普通,材质一般,价格适中,没想象中好', ',')[cast(floor(rand()*5) as int)])
                          end AS neutral_content,
                      case
                          when pi.category_id in ('C001', 'C002') then concat('不太满意，', split('运行卡顿,续航差,做工粗糙,与描述不符,不值这个价', ',')[cast(floor(rand()*5) as int)])
                          when pi.category_id = 'C003' then concat('衣服有问题，', split('尺码不准,面料差,做工粗糙,有色差,穿着不舒服', ',')[cast(floor(rand()*5) as int)])
                          else concat('家居用品不行，', split('材质差,设计不合理,容易坏,性价比低,不推荐购买', ',')[cast(floor(rand()*5) as int)])
                          end AS negative_content
                  FROM ods_order_info oi
                           -- 关联商品表获取类目
                           LEFT JOIN (
                      SELECT product_id AS pi_product_id, category_id
                      FROM ods_product_info
                      WHERE dt='2025-01-26' AND is_delete='0'
                  ) pi ON oi.product_id = pi.pi_product_id
                  WHERE oi.dt='2025-01-26'
                    AND oi.order_status = '已支付'
                    AND oi.pay_time != ''
                    -- 30%的已支付订单生成评价
                    AND rand() < 0.3
              ) t1
     ) t2;

select * from ods_comment_info;
-- TODO 6. 内容信息表（ods_content_info）
DROP TABLE IF EXISTS ods_content_info;
CREATE TABLE IF NOT EXISTS ods_content_info
(
    content_id       STRING COMMENT '内容ID',
    product_id       STRING COMMENT '关联商品ID',
    content_type     STRING COMMENT '内容类型（live-直播、video-短视频、article-图文）',
    publish_time     STRING COMMENT '发布时间（yyyy-MM-dd HH:mm:ss）',
    view_count       INT COMMENT '观看量',
    click_count      INT COMMENT '点击量',
    conversion_count INT COMMENT '转化量（从内容跳转购买的数量）'
) COMMENT '内容营销原始数据'
    PARTITIONED BY (dt STRING COMMENT '分区日期（yyyy-MM-dd）')
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE;

-- 生成内容营销原始数据（最终修正版，解决同层级字段引用错误）
INSERT INTO TABLE ods_content_info PARTITION (dt='2025-01-26')
SELECT
    -- 内容ID：按类型前缀+日期+随机8位数字（唯一标识）
    concat(
            case t2.content_type
                when 'live' then 'L'
                when 'video' then 'V'
                else 'A'
                end,
            regexp_replace('2025-01-26', '-', ''),
            lpad(floor(rand() * 100000000), 8, '0')
    ) AS content_id,
    -- 关联商品ID
    t2.product_id,
    -- 内容类型
    t2.content_type,
    -- 发布时间：当天08:00-22:00
    concat(
            '2025-01-26 ',
            lpad(8 + floor(rand() * 15), 2, '0'), ':',  -- 8-22点
            lpad(floor(rand() * 60), 2, '0'), ':',
            lpad(floor(rand() * 60), 2, '0')
    ) AS publish_time,
    -- 观看量：引用子查询生成的view_count
    t2.view_count,
    -- 点击量：基于子查询的view_count计算
    t2.click_count,
    -- 转化量：基于子查询的click_count计算
    cast(t2.click_count * (0.02 + rand() * 0.06) as int) AS conversion_count
FROM (
         -- 中间子查询：提前生成view_count和click_count，避免同层级引用
         SELECT
             t1.product_id,
             t1.content_type,
             -- 观看量：按内容类型区分量级
             case t1.content_type
                 when 'live' then 5000 + floor(rand() * 95000)  -- 直播：5000-10万
                 when 'video' then 1000 + floor(rand() * 49000)  -- 短视频：1000-5万
                 else 100 + floor(rand() * 9900)                 -- 图文：100-1万
                 end AS view_count,
             -- 点击量：基于当前子查询的view_count计算
             case t1.content_type
                 when 'live' then cast((5000 + floor(rand() * 95000)) * (0.1 + rand() * 0.15) as int)  -- 10%-25%
                 when 'video' then cast((1000 + floor(rand() * 49000)) * (0.05 + rand() * 0.15) as int)  -- 5%-20%
                 else cast((100 + floor(rand() * 9900)) * (0.05 + rand() * 0.15) as int)  -- 5%-20%
                 end AS click_count
         FROM (
                  -- 基础子查询：生成商品与内容类型关联
                  SELECT
                      p.product_id,
                      -- 随机分配内容类型（直播30%、短视频50%、图文20%）
                      case floor(rand() * 10)
                          when 0 then 'live'
                          when 1 then 'live'
                          when 2 then 'live'  -- 30%直播
                          when 3 then 'video'
                          when 4 then 'video'
                          when 5 then 'video'
                          when 6 then 'video'
                          when 7 then 'video'  -- 50%短视频
                          else 'article'       -- 20%图文
                          end AS content_type
                  FROM ods_product_info p
                           -- 每个商品生成1-3条内容
                           LATERAL VIEW
                           explode(split(space(cast(floor(rand() * 3) + 1 AS INT)), ' ')) t2 AS content_num
                  WHERE p.dt='2025-01-26'
                    AND p.is_delete='0'  -- 仅关联有效商品
              ) t1
     ) t2;

select * from ods_content_info;
-- TODO 7. 运营动作表（ods_operation_action）
DROP TABLE IF EXISTS ods_operation_action;
CREATE TABLE IF NOT EXISTS ods_operation_action
(
    action_id     STRING COMMENT '运营动作ID',
    product_id    STRING COMMENT '商品ID',
    action_type   STRING COMMENT '动作类型（new_user_discount-新客折扣、installment_free-分期免息、publish_video-发布短视频）',
    action_time   STRING COMMENT '动作执行时间（yyyy-MM-dd HH:mm:ss）',
    action_params STRING COMMENT '动作参数（JSON格式，如{"discount":"10%"}）'
) COMMENT '商品运营动作原始数据'
    PARTITIONED BY (dt STRING COMMENT '分区日期（yyyy-MM-dd）')
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE;

-- 生成商品运营动作原始数据（覆盖主流运营场景）
INSERT INTO TABLE ods_operation_action PARTITION (dt='2025-01-26')
SELECT
    -- 运营动作ID：O+日期+随机8位数字（唯一标识）
    concat('O', regexp_replace('2025-01-26', '-', ''), lpad(floor(rand() * 100000000), 8, '0')) AS action_id,
    -- 关联商品ID（从有效商品中选择）
    t1.product_id,
    -- 运营动作类型（新客折扣40%、分期免息30%、发布短视频30%）
    t1.action_type,
    -- 动作执行时间：当天09:00-21:00（运营活跃时段）
    concat(
            '2025-01-26 ',
            lpad(9 + floor(rand() * 13), 2, '0'), ':',  -- 9-21点
            lpad(floor(rand() * 60), 2, '0'), ':',
            lpad(floor(rand() * 60), 2, '0')
    ) AS action_time,
    -- 动作参数（JSON格式，按类型差异化）
    case t1.action_type
        when 'new_user_discount' then
            concat('{"discount":"', cast(5 + floor(rand() * 16) as string), '%","min_price":"',
                   cast(50 + floor(rand() * 950) as string), '"}')  -- 折扣5%-20%，最低门槛50-1000元
        when 'installment_free' then
            concat('{"periods":"', cast(3 + floor(rand() * 4) as string), '","max_amount":"',
                   cast(1000 + floor(rand() * 9000) as string), '"}')  -- 分期期数3-6期，最高金额1000-10000元
        else  -- publish_video
            concat('{"platform":"', array('抖音','快手','视频号','小红书')[cast(floor(rand()*4) as int)],
                   '","duration":"', cast(15 + floor(rand() * 46) as string), '"}')  -- 平台随机，时长15-60秒
        end AS action_params
FROM (
         -- 子查询：关联商品与运营动作类型
         SELECT
             p.product_id,
             -- 随机分配动作类型（新客折扣40%、分期免息30%、发布短视频30%）
             case floor(rand() * 10)
                 when 0 then 'new_user_discount'
                 when 1 then 'new_user_discount'
                 when 2 then 'new_user_discount'
                 when 3 then 'new_user_discount'  -- 40%新客折扣
                 when 4 then 'installment_free'
                 when 5 then 'installment_free'
                 when 6 then 'installment_free'  -- 30%分期免息
                 else 'publish_video'  -- 30%发布短视频
                 end AS action_type
         FROM ods_product_info p
                  -- 每个商品生成1-2个运营动作（重点商品多运营策略）
                  LATERAL VIEW
                  explode(split(space(cast(floor(rand() * 2) + 1 AS INT)), ' ')) t2 AS action_num
         WHERE p.dt='2025-01-26'
           AND p.is_delete='0'  -- 仅关联有效商品
     ) t1;

select * from ods_operation_action;