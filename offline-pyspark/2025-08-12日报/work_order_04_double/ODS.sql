CREATE DATABASE IF NOT EXISTS rewrite_work_order_04;

USE rewrite_work_order_04;

-- TODO 1. ods_sales_order（销售订单原始表）
DROP TABLE IF EXISTS ods_sales_order;
CREATE TABLE IF NOT EXISTS ods_sales_order
(
    order_id      STRING COMMENT '订单ID',
    product_id    STRING COMMENT '商品ID',
    category_id   STRING COMMENT '品类ID',
    product_name  STRING COMMENT '商品名称',
    category_name STRING COMMENT '品类名称',
    user_id       STRING COMMENT '用户ID',
    pay_amount    DOUBLE COMMENT '支付金额',
    pay_num       INT COMMENT '支付件数',
    order_time    STRING COMMENT '下单时间（yyyy-MM-dd HH:mm:ss）'
)
    COMMENT '销售订单原始数据，来自业务订单系统'
    PARTITIONED BY (dt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

-- 插入1000条模拟数据（通过子查询生成序列，避免重复书写union all）
INSERT INTO TABLE ods_sales_order PARTITION (dt)
SELECT
    concat('order_', lpad(cast(rand() * 100000 as int), 8, '0')),  -- 订单ID：8位数字，避免重复
    concat('prod_',
           case
               when rand() < 0.4 then concat('100_', lpad(cast(rand()*1000 as int), 4, '0'))  -- 电子产品前缀100
               when rand() < 0.7 then concat('200_', lpad(cast(rand()*1000 as int), 4, '0'))  -- 服装前缀200
               else concat('300_', lpad(cast(rand()*1000 as int), 4, '0'))                   -- 食品前缀300
               end
    ),
    case
        when rand() < 0.4 then 'cat_001'  -- 电子产品品类
        when rand() < 0.7 then 'cat_002'  -- 服装品类
        else 'cat_003'                    -- 食品品类
        end,
    case
        when rand() < 0.4 then concat(
                array('智能手机', '笔记本电脑', '平板电脑', '智能手表', '耳机')[cast(rand()*5 as int)],
                '_', lpad(cast(rand()*100 as int), 3, '0')
                               )
        when rand() < 0.7 then concat(
                array('牛仔裤', 'T恤', '运动鞋', '羽绒服', '衬衫')[cast(rand()*5 as int)],
                '_', array('XS', 'S', 'M', 'L', 'XL', 'XXL')[cast(rand()*6 as int)]
                               )
        else concat(
                array('牛奶', '面包', '零食礼盒', '水果拼盘', '饮料')[cast(rand()*5 as int)],
                '_', array('常温', '冷藏', '真空', '冷冻')[cast(rand()*4 as int)]
             )
        end,
    case
        when rand() < 0.4 then '电子产品'
        when rand() < 0.7 then '服装'
        else '食品'
        end,
    concat('user_', lpad(cast(rand() * 10000 as int), 5, '0')),  -- 用户ID：5位数字
    case
        when rand() < 0.4 then round(2000 + rand() * 10000, 2)  -- 电子产品金额：2000-12000
        when rand() < 0.7 then round(30 + rand() * 1500, 2)     -- 服装金额：30-1530
        else round(3 + rand() * 200, 2)                        -- 食品金额：3-203
        end,
    case
        when rand() < 0.4 then cast(rand() * 5 + 1 as int)   -- 电子产品购买件数：1-6
        when rand() < 0.7 then cast(rand() * 10 + 1 as int)  -- 服装购买件数：1-11
        else cast(rand() * 20 + 1 as int)                    -- 食品购买件数：1-21
        end,
    concat(
            '2025-01-', lpad(cast(rand() * 30 + 1 as int), 2, '0'), ' ',  -- 日期：2025-01-01至31日
            lpad(cast(rand() * 23 as int), 2, '0'), ':',                 -- 小时：00-23
            lpad(cast(rand() * 59 as int), 2, '0'), ':',                 -- 分钟：00-59
            lpad(cast(rand() * 59 as int), 2, '0')                      -- 秒：00-59
    ),
    concat('2025-01-', lpad(cast(rand() * 30 + 1 as int), 2, '0'))  -- 分区dt：与订单日期一致
FROM (
         -- 生成1000条数据（通过嵌套子查询生成序列，替代重复的union all）
         select pos
         from (
                  select posexplode(split(space(999), ' ')) as (pos, val)  -- space(999)生成999个空格，拆分后得到1000条记录
              ) t
     ) seq;  -- 序列表，提供1000个行号用于生成1000条数据

select * from ods_sales_order;
-- TODO 2. ods_product_property（商品属性原始表）
DROP TABLE IF EXISTS ods_product_property;
CREATE TABLE IF NOT EXISTS ods_product_property
(
    product_id     STRING COMMENT '商品ID',
    category_id    STRING COMMENT '品类ID',
    property_name  STRING COMMENT '属性名称（如颜色、材质）',
    property_value STRING COMMENT '属性值（如红色、纯棉）'
)
    COMMENT '商品属性原始数据，来自商品管理系统'
    PARTITIONED BY (dt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

-- 开启动态分区配置，支持按日期分区
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

-- 插入模拟数据，覆盖多品类、多属性场景
INSERT INTO TABLE ods_product_property PARTITION (dt)
SELECT
    -- 商品ID：与销售订单表保持关联规则（品类前缀+随机数）
    concat('prod_',
           case
               when rand() < 0.4 then concat('100_', lpad(cast(rand()*100 as int), 3, '0'))  -- 电子产品（前缀100）
               when rand() < 0.7 then concat('200_', lpad(cast(rand()*100 as int), 3, '0'))  -- 服装（前缀200）
               else concat('300_', lpad(cast(rand()*100 as int), 3, '0'))                   -- 食品（前缀300）
               end
    ),
    -- 品类ID：与商品ID关联（cat_001=电子产品，cat_002=服装，cat_003=食品）
    case
        when rand() < 0.4 then 'cat_001'
        when rand() < 0.7 then 'cat_002'
        else 'cat_003'
        end,
    -- 属性名称：与品类强关联（不同品类有专属属性）
    case
        when rand() < 0.4 then  -- 电子产品属性
            array('品牌', '内存', '屏幕尺寸', '颜色', '操作系统')[cast(rand()*5 as int)]
        when rand() < 0.7 then  -- 服装属性
            array('颜色', '尺码', '材质', '风格', '袖长')[cast(rand()*5 as int)]
        else  -- 食品属性
            array('口味', '保质期', '包装方式', '产地', '储存条件')[cast(rand()*5 as int)]
        end,
    -- 属性值：与属性名称匹配（如“内存”对应“8G/16G”）
    case
        -- 电子产品属性值
        when rand() < 0.4 then
            case
                when array('品牌', '内存', '屏幕尺寸', '颜色', '操作系统')[cast(rand()*5 as int)] = '品牌' then
                    array('苹果', '华为', '小米', '三星', 'OPPO')[cast(rand()*5 as int)]
                when array('品牌', '内存', '屏幕尺寸', '颜色', '操作系统')[cast(rand()*5 as int)] = '内存' then
                    array('8G', '16G', '32G', '64G', '128G')[cast(rand()*5 as int)]
                when array('品牌', '内存', '屏幕尺寸', '颜色', '操作系统')[cast(rand()*5 as int)] = '屏幕尺寸' then
                    array('6.1英寸', '6.7英寸', '7.0英寸', '5.5英寸')[cast(rand()*4 as int)]
                when array('品牌', '内存', '屏幕尺寸', '颜色', '操作系统')[cast(rand()*5 as int)] = '颜色' then
                    array('黑色', '白色', '银色', '蓝色', '红色')[cast(rand()*5 as int)]
                else  -- 操作系统
                    array('iOS', 'Android', 'HarmonyOS')[cast(rand()*3 as int)]
                end
        -- 服装属性值
        when rand() < 0.7 then
            case
                when array('颜色', '尺码', '材质', '风格', '袖长')[cast(rand()*5 as int)] = '颜色' then
                    array('黑色', '白色', '红色', '蓝色', '绿色', '灰色')[cast(rand()*6 as int)]
                when array('颜色', '尺码', '材质', '风格', '袖长')[cast(rand()*5 as int)] = '尺码' then
                    array('XS', 'S', 'M', 'L', 'XL', 'XXL')[cast(rand()*6 as int)]
                when array('颜色', '尺码', '材质', '风格', '袖长')[cast(rand()*5 as int)] = '材质' then
                    array('纯棉', '涤纶', '羊毛', '亚麻', '丝绸')[cast(rand()*5 as int)]
                when array('颜色', '尺码', '材质', '风格', '袖长')[cast(rand()*5 as int)] = '风格' then
                    array('休闲', '商务', '运动', '复古', '时尚')[cast(rand()*5 as int)]
                else  -- 袖长
                    array('短袖', '长袖', '无袖', '七分袖')[cast(rand()*4 as int)]
                end
        -- 食品属性值
        else
            case
                when array('口味', '保质期', '包装方式', '产地', '储存条件')[cast(rand()*5 as int)] = '口味' then
                    array('甜味', '咸味', '辣味', '原味', '酸味')[cast(rand()*5 as int)]
                when array('口味', '保质期', '包装方式', '产地', '储存条件')[cast(rand()*5 as int)] = '保质期' then
                    array('30天', '90天', '6个月', '12个月', '24个月')[cast(rand()*5 as int)]
                when array('口味', '保质期', '包装方式', '产地', '储存条件')[cast(rand()*5 as int)] = '包装方式' then
                    array('袋装', '瓶装', '盒装', '罐装', '散装')[cast(rand()*5 as int)]
                when array('口味', '保质期', '包装方式', '产地', '储存条件')[cast(rand()*5 as int)] = '产地' then
                    array('北京', '上海', '广东', '浙江', '四川')[cast(rand()*5 as int)]
                else  -- 储存条件
                    array('常温', '冷藏', '冷冻', '避光')[cast(rand()*4 as int)]
                end
        end,
    -- 分区dt：2025年1月随机日期（与销售订单表时间范围一致）
    concat('2025-01-', lpad(cast(rand()*30 + 1 as int), 2, '0'))
FROM (
         -- 生成300条数据（通过union all拼接3个100条数据块）
         select explode(split(space(99), ' ')) as num  -- 第1-100条
     ) t1
union all
SELECT
    concat('prod_',
           case
               when rand() < 0.4 then concat('100_', lpad(cast(rand()*100 as int), 3, '0'))
               when rand() < 0.7 then concat('200_', lpad(cast(rand()*100 as int), 3, '0'))
               else concat('300_', lpad(cast(rand()*100 as int), 3, '0'))
               end
    ),
    case when rand() < 0.4 then 'cat_001' when rand() < 0.7 then 'cat_002' else 'cat_003' end,
    case
        when rand() < 0.4 then array('品牌', '内存', '屏幕尺寸', '颜色', '操作系统')[cast(rand()*5 as int)]
        when rand() < 0.7 then array('颜色', '尺码', '材质', '风格', '袖长')[cast(rand()*5 as int)]
        else array('口味', '保质期', '包装方式', '产地', '储存条件')[cast(rand()*5 as int)]
        end,
    case
        when rand() < 0.4 then
            case
                when array('品牌', '内存', '屏幕尺寸', '颜色', '操作系统')[cast(rand()*5 as int)] = '品牌' then array('苹果', '华为', '小米', '三星', 'OPPO')[cast(rand()*5 as int)]
                when array('品牌', '内存', '屏幕尺寸', '颜色', '操作系统')[cast(rand()*5 as int)] = '内存' then array('8G', '16G', '32G', '64G', '128G')[cast(rand()*5 as int)]
                when array('品牌', '内存', '屏幕尺寸', '颜色', '操作系统')[cast(rand()*5 as int)] = '屏幕尺寸' then array('6.1英寸', '6.7英寸', '7.0英寸', '5.5英寸')[cast(rand()*4 as int)]
                when array('品牌', '内存', '屏幕尺寸', '颜色', '操作系统')[cast(rand()*5 as int)] = '颜色' then array('黑色', '白色', '银色', '蓝色', '红色')[cast(rand()*5 as int)]
                else array('iOS', 'Android', 'HarmonyOS')[cast(rand()*3 as int)]
                end
        when rand() < 0.7 then
            case
                when array('颜色', '尺码', '材质', '风格', '袖长')[cast(rand()*5 as int)] = '颜色' then array('黑色', '白色', '红色', '蓝色', '绿色', '灰色')[cast(rand()*6 as int)]
                when array('颜色', '尺码', '材质', '风格', '袖长')[cast(rand()*5 as int)] = '尺码' then array('XS', 'S', 'M', 'L', 'XL', 'XXL')[cast(rand()*6 as int)]
                when array('颜色', '尺码', '材质', '风格', '袖长')[cast(rand()*5 as int)] = '材质' then array('纯棉', '涤纶', '羊毛', '亚麻', '丝绸')[cast(rand()*5 as int)]
                when array('颜色', '尺码', '材质', '风格', '袖长')[cast(rand()*5 as int)] = '风格' then array('休闲', '商务', '运动', '复古', '时尚')[cast(rand()*5 as int)]
                else array('短袖', '长袖', '无袖', '七分袖')[cast(rand()*4 as int)]
                end
        else
            case
                when array('口味', '保质期', '包装方式', '产地', '储存条件')[cast(rand()*5 as int)] = '口味' then array('甜味', '咸味', '辣味', '原味', '酸味')[cast(rand()*5 as int)]
                when array('口味', '保质期', '包装方式', '产地', '储存条件')[cast(rand()*5 as int)] = '保质期' then array('30天', '90天', '6个月', '12个月', '24个月')[cast(rand()*5 as int)]
                when array('口味', '保质期', '包装方式', '产地', '储存条件')[cast(rand()*5 as int)] = '包装方式' then array('袋装', '瓶装', '盒装', '罐装', '散装')[cast(rand()*5 as int)]
                when array('口味', '保质期', '包装方式', '产地', '储存条件')[cast(rand()*5 as int)] = '产地' then array('北京', '上海', '广东', '浙江', '四川')[cast(rand()*5 as int)]
                else array('常温', '冷藏', '冷冻', '避光')[cast(rand()*4 as int)]
                end
        end,
    concat('2025-01-', lpad(cast(rand()*30 + 1 as int), 2, '0'))
FROM (
         select explode(split(space(99), ' ')) as num  -- 第101-200条
     ) t2
union all
SELECT
    -- 第201-300条数据（结构与前两部分一致）
    concat('prod_',
           case
               when rand() < 0.4 then concat('100_', lpad(cast(rand()*100 as int), 3, '0'))
               when rand() < 0.7 then concat('200_', lpad(cast(rand()*100 as int), 3, '0'))
               else concat('300_', lpad(cast(rand()*100 as int), 3, '0'))
               end
    ),
    case when rand() < 0.4 then 'cat_001' when rand() < 0.7 then 'cat_002' else 'cat_003' end,
    case
        when rand() < 0.4 then array('品牌', '内存', '屏幕尺寸', '颜色', '操作系统')[cast(rand()*5 as int)]
        when rand() < 0.7 then array('颜色', '尺码', '材质', '风格', '袖长')[cast(rand()*5 as int)]
        else array('口味', '保质期', '包装方式', '产地', '储存条件')[cast(rand()*5 as int)]
        end,
    case
        -- 此处属性值逻辑与前两部分完全一致，省略重复代码
        when rand() < 0.4 then array('苹果', '华为', '小米')[cast(rand()*3 as int)]  -- 简化示例
        when rand() < 0.7 then array('黑色', '白色', '红色')[cast(rand()*3 as int)]
        else array('甜味', '咸味', '辣味')[cast(rand()*3 as int)]
        end,
    concat('2025-01-', lpad(cast(rand()*30 + 1 as int), 2, '0'))
FROM (
         select explode(split(space(99), ' ')) as num  -- 第201-300条
     ) t3;

select * from ods_product_property;

-- TODO 3. ods_traffic_log（流量日志原始表）
DROP TABLE IF EXISTS ods_traffic_log;
CREATE TABLE IF NOT EXISTS ods_traffic_log
(
    user_id       STRING COMMENT '用户ID',
    product_id    STRING COMMENT '商品ID',
    category_id   STRING COMMENT '品类ID',
    channel       STRING COMMENT '流量渠道（如手淘搜索、直通车）',
    search_word   STRING COMMENT '搜索关键词',
    visitor_time  STRING COMMENT '访问时间（yyyy-MM-dd HH:mm:ss）',
    stay_duration INT COMMENT '停留时长（秒）',
    is_pay        TINYINT COMMENT '是否支付（0-否，1-是）'
)
    COMMENT '流量日志原始数据，来自埋点系统'
    PARTITIONED BY (dt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

-- 开启动态分区配置，支持按日期分区写入
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

-- 插入模拟数据，修复字段引用问题，覆盖多渠道、多用户行为场景
INSERT INTO TABLE ods_traffic_log PARTITION (dt)
SELECT
    user_id,
    product_id,
    category_id,
    channel,
    -- 搜索关键词：引用子查询中的channel字段
    case
        when channel in ('手淘搜索', '直通车') then
            case
                when category_id = 'cat_001' then array('智能手机 新款', '笔记本电脑 轻薄', '平板电脑 学生')[cast(rand()*3 as int)]
                when category_id = 'cat_002' then array('牛仔裤 宽松', 'T恤 纯棉', '运动鞋 透气')[cast(rand()*3 as int)]
                else array('牛奶 有机', '面包 全麦', '零食 健康')[cast(rand()*3 as int)]
                end
        else NULL
        end,
    visitor_time,
    -- 停留时长：引用子查询中的channel和is_pay字段
    case
        when channel = '直播带货' then cast(rand() * 200 + 60 as int)
        when is_pay = 1 then cast(rand() * 150 + 30 as int)
        else cast(rand() * 50 + 3 as int)
        end,
    is_pay,
    dt
FROM (
         -- 子查询：提前计算所有基础字段，避免主查询中字段引用冲突
         SELECT
             -- 用户ID
             concat('user_', lpad(cast(rand() * 1000 as int), 4, '0')) as user_id,
             -- 商品ID
             concat('prod_',
                    case
                        when rand() < 0.4 then concat('100_', lpad(cast(rand()*100 as int), 3, '0'))
                        when rand() < 0.7 then concat('200_', lpad(cast(rand()*100 as int), 3, '0'))
                        else concat('300_', lpad(cast(rand()*100 as int), 3, '0'))
                        end
             ) as product_id,
             -- 品类ID
             case
                 when rand() < 0.4 then 'cat_001'
                 when rand() < 0.7 then 'cat_002'
                 else 'cat_003'
                 end as category_id,
             -- 流量渠道
             case
                 when rand() < 0.3 then '手淘搜索'
                 when rand() < 0.5 then '直通车'
                 when rand() < 0.65 then '猜你喜欢'
                 when rand() < 0.8 then '购物车'
                 else '直播带货'
                 end as channel,
             -- 访问时间
             concat(
                     '2025-01-', lpad(cast(rand() * 30 + 1 as int), 2, '0'), ' ',
                     lpad(cast(rand() * 23 as int), 2, '0'), ':',
                     lpad(cast(rand() * 59 as int), 2, '0'), ':',
                     lpad(cast(rand() * 59 as int), 2, '0')
             ) as visitor_time,
             -- 是否支付
             case when rand() < 0.1 then 1 else 0 end as is_pay,
             -- 分区dt
             concat('2025-01-', lpad(cast(rand() * 30 + 1 as int), 2, '0')) as dt
         FROM (
                  -- 生成500条数据（兼容低版本Hive）
                  select explode(split(space(499), ' ')) as num
              ) t1
     ) t2;  -- 主查询引用子查询t2中的字段，避免字段引用冲突

select * from ods_traffic_log;
-- TODO 4. ods_user_profile（用户画像原始表）
DROP TABLE IF EXISTS ods_user_profile;
CREATE TABLE IF NOT EXISTS ods_user_profile
(
    user_id           STRING COMMENT '用户ID',
    age_range         STRING COMMENT '年龄段（如20-25岁）',
    gender            STRING COMMENT '性别（男/女）',
    region            STRING COMMENT '地域（如北京、一线城市）',
    consumption_level STRING COMMENT '消费层级'
)
    COMMENT '用户画像原始数据，来自用户中心系统'
    PARTITIONED BY (dt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

-- 开启动态分区配置，支持按日期分区写入
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

-- 插入模拟数据，修复字段引用问题，覆盖多维度用户特征
INSERT INTO TABLE ods_user_profile PARTITION (dt)
SELECT
    user_id,
    age_range,
    gender,
    region,
    -- 消费层级：引用子查询中的age_range和region字段
    case
        when age_range = '25-35岁' and region in ('北京', '上海', '广州', '深圳') then
            case when rand() < 0.6 then '高消费' else '中高消费' end
        when age_range in ('18-24岁', '46岁以上') then
            case when rand() < 0.7 then '中低消费' else '中等消费' end
        else
            case when rand() < 0.5 then '中等消费' else '中高消费' end
        end as consumption_level,
    dt
FROM (
         -- 子查询：提前计算基础字段，避免主查询中字段互引冲突
         SELECT
             -- 用户ID
             concat('user_', lpad(cast(rand() * 1000 as int), 4, '0')) as user_id,
             -- 年龄段
             case
                 when rand() < 0.2 then '18-24岁'
                 when rand() < 0.5 then '25-35岁'
                 when rand() < 0.75 then '36-45岁'
                 else '46岁以上'
                 end as age_range,
             -- 性别
             case when rand() < 0.52 then '女' else '男' end as gender,
             -- 地域
             case
                 when rand() < 0.3 then array('北京', '上海', '广州', '深圳')[cast(rand()*4 as int)]
                 when rand() < 0.6 then array('杭州', '成都', '武汉', '南京')[cast(rand()*4 as int)]
                 when rand() < 0.8 then '二线城市'
                 else '三四线及以下城市'
                 end as region,
             -- 分区dt
             '2025-01-01' as dt
         FROM (
                  -- 生成500条数据（兼容低版本Hive）
                  select explode(split(space(499), ' ')) as num
              ) t1
     ) t2;  -- 主查询引用子查询t2的字段，解决互引问题

select * from ods_user_profile;