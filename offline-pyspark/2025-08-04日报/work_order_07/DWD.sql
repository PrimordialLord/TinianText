USE work_order_07;

-- DWD 商品基础明细（dwd_product_base_detail）
DROP TABLE IF EXISTS dwd_product_base_detail;
CREATE TABLE IF NOT EXISTS dwd_product_base_detail
(
    product_id    STRING COMMENT '商品唯一标识',
    product_name  STRING COMMENT '商品名称',
    category_id   STRING COMMENT '商品分类ID',
    category_name STRING COMMENT '商品分类名称',
    brand_id      STRING COMMENT '品牌ID',
    brand_name    STRING COMMENT '品牌名称',
    price         DOUBLE COMMENT '商品售价',
    etl_time      STRING COMMENT 'ETL处理时间'
)
    COMMENT 'DWD层商品基础明细数据'
    PARTITIONED BY (dt STRING COMMENT '数据日期')
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\t'
    STORED AS PARQUET;

-- 从ODS层抽取数据到DWD层商品基础明细表（保留核心字段，去除冗余）
INSERT OVERWRITE TABLE dwd_product_base_detail PARTITION (dt = '2025-08-03')
SELECT
    -- 商品唯一标识（直接复用ODS层清洗后的ID）
    opb.product_id,

    -- 商品名称（保留原始名称，已在ODS层完成格式化）
    opb.product_name,

    -- 商品分类ID（关联分类体系）
    opb.category_id,

    -- 商品分类名称（冗余存储，便于分析）
    opb.category_name,

    -- 品牌ID（关联品牌体系）
    opb.brand_id,

    -- 品牌名称（冗余存储，提升查询效率）
    opb.brand_name,

    -- 商品售价（保留两位小数，确保精度）
    round(opb.price, 2)                                    AS price,

    -- ETL处理时间（记录当前转换时间）
    from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss') AS etl_time
FROM
    -- 源表：ODS层商品基础信息表（指定分区）
    ods_product_base opb
WHERE
  -- 过滤当前分区数据
    opb.dt = '2025-08-03'
  -- 数据清洗：过滤无效商品ID（确保主键非空）
  AND opb.product_id IS NOT NULL
  AND opb.product_id != ''
  -- 清洗异常价格（排除0或负数价格）
  AND opb.price > 0;


select *
from dwd_product_base_detail;

-- DWD 商品行为明细（dwd_product_behavior_detail）
DROP TABLE IF EXISTS dwd_product_behavior_detail;
CREATE TABLE IF NOT EXISTS dwd_product_behavior_detail
(
    product_id         STRING COMMENT '商品ID',
    visitor_count      BIGINT COMMENT '访客数',
    sales_amount       DOUBLE COMMENT '销售金额',
    sales_volume       BIGINT COMMENT '销量',
    conversion_rate    DOUBLE COMMENT '转化率(%)',
    new_customer_count BIGINT COMMENT '新增客户数',
    traffic_index      DOUBLE COMMENT '流量指标原始值',
    conversion_index   DOUBLE COMMENT '转化指标原始值',
    content_index      DOUBLE COMMENT '内容指标原始值',
    new_customer_index DOUBLE COMMENT '拉新指标原始值',
    service_index      DOUBLE COMMENT '服务指标原始值',
    etl_time           STRING COMMENT 'ETL处理时间'
)
    COMMENT 'DWD层商品行为明细数据'
    PARTITIONED BY (dt STRING COMMENT '数据日期')
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\t'
    STORED AS PARQUET;


-- 从ODS层抽取并清洗数据到DWD层商品行为明细表
INSERT OVERWRITE TABLE dwd_product_behavior_detail PARTITION (dt = '2025-08-03')
SELECT
    -- 商品ID（与商品基础表关联，确保非空）
    opb.product_id,

    -- 访客数（清洗异常值：确保≥0）
    case when opb.visitor_count < 0 then 0 else opb.visitor_count end           AS visitor_count,

    -- 销售金额（清洗异常值：确保≥0，保留两位小数）
    round(case when opb.sales_amount < 0 then 0 else opb.sales_amount end, 2)   AS sales_amount,

    -- 销量（清洗异常值：确保≥0）
    case when opb.sales_volume < 0 then 0 else opb.sales_volume end             AS sales_volume,

    -- 转化率（清洗异常值：限制在0-100%，保留两位小数）
    round(
            case
                when opb.conversion_rate < 0 then 0
                when opb.conversion_rate > 100 then 100
                else opb.conversion_rate
                end, 2
    )                                                                           AS conversion_rate,

    -- 新增客户数（清洗异常值：确保≥0）
    case when opb.new_customer_count < 0 then 0 else opb.new_customer_count end AS new_customer_count,

    -- 流量指标（清洗异常值：限制在0-100）
    round(
            case
                when opb.traffic_index < 0 then 0
                when opb.traffic_index > 100 then 100
                else opb.traffic_index
                end, 2
    )                                                                           AS traffic_index,

    -- 转化指标（清洗异常值：限制在0-100）
    round(
            case
                when opb.conversion_index < 0 then 0
                when opb.conversion_index > 100 then 100
                else opb.conversion_index
                end, 2
    )                                                                           AS conversion_index,

    -- 内容指标（清洗异常值：限制在0-100）
    round(
            case
                when opb.content_index < 0 then 0
                when opb.content_index > 100 then 100
                else opb.content_index
                end, 2
    )                                                                           AS content_index,

    -- 拉新指标（清洗异常值：限制在0-100）
    round(
            case
                when opb.new_customer_index < 0 then 0
                when opb.new_customer_index > 100 then 100
                else opb.new_customer_index
                end, 2
    )                                                                           AS new_customer_index,

    -- 服务指标（清洗异常值：限制在0-100）
    round(
            case
                when opb.service_index < 0 then 0
                when opb.service_index > 100 then 100
                else opb.service_index
                end, 2
    )                                                                           AS service_index,

    -- 记录DWD层ETL处理时间
    from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss')                      AS etl_time
FROM
    -- 源表：ODS层商品行为表（指定分区）
    ods_product_behavior opb
WHERE
  -- 过滤当前分区数据
    opb.dt = '2025-08-03'
  -- 核心过滤：排除商品ID为空的无效数据
  AND opb.product_id IS NOT NULL
  AND opb.product_id != ''
  -- 业务校验：访客数不能为负数（基础过滤）
  AND opb.visitor_count >= 0;


select *
from dwd_product_behavior_detail;

-- DWD 竞品对比明细（dwd_competitor_detail）
DROP TABLE IF EXISTS dwd_competitor_detail;
CREATE TABLE IF NOT EXISTS dwd_competitor_detail
(
    product_id            STRING COMMENT '本品ID',
    competitor_product_id STRING COMMENT '竞品ID',
    dimension             STRING COMMENT '对比维度(流量获取/转化等)',
    self_value            DOUBLE COMMENT '本品指标值',
    competitor_value      DOUBLE COMMENT '竞品指标值',
    etl_time              STRING COMMENT 'ETL处理时间'
)
    COMMENT 'DWD层竞品对比明细数据'
    PARTITIONED BY (dt STRING COMMENT '数据日期')
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\t'
    STORED AS PARQUET;

-- 从ODS层抽取并清洗数据到DWD层竞品对比明细表
INSERT OVERWRITE TABLE dwd_competitor_detail PARTITION (dt = '2025-08-03')
SELECT
    -- 本品ID（与商品基础表关联，确保非空）
    oci.product_id,

    -- 竞品ID（确保格式规范，非空）
    oci.competitor_product_id,

    -- 对比维度标准化（统一维度名称，便于分析）
    case oci.dimension
        when '价格' then '价格竞争力'
        when '销量' then '市场销量'
        when '好评率' then '用户好评率'
        when '功能完整性' then '产品功能'
        when '售后服务' then '服务质量'
        when '市场占有率' then '市场份额'
        when '用户复购率' then '用户忠诚度'
        when '品牌知名度' then '品牌影响力'
        else '其他维度' -- 统一非标准维度名称
        end                                                AS dimension,

    -- 本品指标值（清洗异常值，保留合理精度）
    round(
            case
                when oci.self_value < 0 then 0 -- 指标值不能为负
            -- 针对不同维度设置合理上限（根据业务常识）
                when oci.dimension in ('好评率', '用户复购率', '市场占有率') and oci.self_value > 100
                    then 100 -- 百分比类指标上限100%
                when oci.dimension in ('功能完整性', '售后服务') and oci.self_value > 10
                    then 10 -- 评分类指标上限10分
                else oci.self_value
                end, 2
    )                                                      AS self_value,

    -- 竞品指标值（同本品规则清洗）
    round(
            case
                when oci.competitor_value < 0 then 0
                when oci.dimension in ('好评率', '用户复购率', '市场占有率') and oci.competitor_value > 100
                    then 100
                when oci.dimension in ('功能完整性', '售后服务') and oci.competitor_value > 10
                    then 10
                else oci.competitor_value
                end, 2
    )                                                      AS competitor_value,

    -- DWD层ETL处理时间（记录转换时间）
    from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss') AS etl_time
FROM
    -- 源表：ODS层竞品对比原始表（指定分区）
    ods_competitor_info oci
WHERE
  -- 过滤当前分区数据
    oci.dt = '2025-08-03'
  -- 核心过滤：排除本品ID或竞品ID为空的无效数据
  AND oci.product_id IS NOT NULL
  AND oci.product_id != ''
  AND oci.competitor_product_id IS NOT NULL
  AND oci.competitor_product_id != ''
  -- 过滤无效维度（排除空维度或无意义值）
  AND oci.dimension IS NOT NULL
  AND oci.dimension NOT IN ('', '未知', '无');

select *
from dwd_competitor_detail;



