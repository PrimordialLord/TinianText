USE rewrite_work_order_03;

-- TODO 1. 商品明细宽表（dwd_product_detail）
DROP TABLE IF EXISTS dwd_product_detail;
CREATE EXTERNAL TABLE IF NOT EXISTS dwd_product_detail
(
    product_id    STRING COMMENT '商品ID',
    product_name  STRING COMMENT '商品名称',
    category_id   STRING COMMENT '类目ID',
    category_name STRING COMMENT '类目名称',
    price         DECIMAL(10, 2) COMMENT '商品标价',
    status        STRING COMMENT '商品状态',
    create_date   STRING COMMENT '商品创建日期（yyyy-MM-dd）'
)
    COMMENT '商品清洗后明细宽表'
    PARTITIONED BY (dt STRING)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
        WITH SERDEPROPERTIES (
        'field.delim' = '\t',
        'serialization.format' = '\t'
        )
    STORED AS ORC; -- 采用ORC格式，压缩率高，查询效率高



SELECT * FROM dwd_product_detail;
-- TODO 2. SKU 明细宽表（dwd_sku_detail）
DROP TABLE IF EXISTS dwd_sku_detail;
CREATE EXTERNAL TABLE IF NOT EXISTS dwd_sku_detail
(
    sku_id       STRING COMMENT 'SKU ID',
    product_id   STRING COMMENT '商品ID',
    sku_name     STRING COMMENT 'SKU名称',
    sku_attr_map MAP<STRING,STRING> COMMENT 'SKU属性Map（如{"颜色":"红色","尺寸":"XL"}）',
    sku_price    DECIMAL(10, 2) COMMENT 'SKU售价',

    stock_num    BIGINT COMMENT '库存数量'
)
    COMMENT 'SKU清洗后明细宽表'
    PARTITIONED BY (dt STRING)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
        WITH SERDEPROPERTIES (
        'field.delim' = '\t',
        'serialization.format' = '\t'
        )
    STORED AS ORC;



select * from dwd_sku_detail;
-- TODO 3. 用户行为明细宽表（dwd_user_behavior_detail）
DROP TABLE IF EXISTS dwd_user_behavior_detail;
CREATE EXTERNAL TABLE IF NOT EXISTS dwd_user_behavior_detail
(
    user_id       STRING COMMENT '用户ID',
    product_id    STRING COMMENT '商品ID',
    sku_id        STRING COMMENT 'SKU ID',
    behavior_type STRING COMMENT '行为类型',
    behavior_time STRING COMMENT '行为时间',
    behavior_date STRING COMMENT '行为日期',
    hour          STRING COMMENT '行为小时（00-23）',
    channel       STRING COMMENT '来源渠道'
)
    COMMENT '用户行为清洗后明细宽表'
    PARTITIONED BY (dt STRING)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
        WITH SERDEPROPERTIES (
        'field.delim' = '\t',
        'serialization.format' = '\t'
        )
    STORED AS ORC;



select * from dwd_user_behavior_detail;
-- TODO 4. 内容 - 商品关联明细（dwd_content_product_rel）
DROP TABLE IF EXISTS dwd_content_product_rel;
CREATE EXTERNAL TABLE IF NOT EXISTS dwd_content_product_rel
(
    content_id     STRING COMMENT '内容ID',
    product_id     STRING COMMENT '商品ID',
    content_type   STRING COMMENT '内容类型',
    view_count     BIGINT COMMENT '播放量',
    interact_count BIGINT COMMENT '互动量',
    publish_time   STRING COMMENT '发布时间',
    publish_date   STRING COMMENT '发布日期'
)
    COMMENT '内容与商品关联明细宽表'
    PARTITIONED BY (dt STRING)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
        WITH SERDEPROPERTIES (
        'field.delim' = '\t',
        'serialization.format' = '\t'
        )
    STORED AS ORC;



select * from dwd_content_product_rel;
-- TODO
DROP TABLE  IF EXISTS dwd_review_detail;
CREATE EXTERNAL TABLE IF NOT EXISTS dwd_review_detail
(
    review_id   STRING COMMENT '评价ID',
    product_id  STRING COMMENT '商品ID',
    sku_id      STRING COMMENT 'SKU ID',
    user_id     STRING COMMENT '用户ID',
    score       INT COMMENT '评分',
    content     STRING COMMENT '评价内容',
    create_time STRING COMMENT '评价时间',
    create_date STRING COMMENT '评价日期',
    user_type   STRING COMMENT '用户类型'
)
    COMMENT '评价清洗后明细宽表'
    PARTITIONED BY (dt STRING)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
        WITH SERDEPROPERTIES (
        'field.delim' = '\t',
        'serialization.format' = '\t'
        )
    STORED AS ORC;




select * from dwd_review_detail;