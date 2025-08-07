USE work_order_04_double;

-- todo 1. ads_category_360_overview（品类 360 全景表）
CREATE TABLE ads_category_360_overview (
                                           category_id STRING COMMENT '品类ID',
                                           category_name STRING COMMENT '品类名称',
                                           sales_overview STRING COMMENT '销售概况（JSON格式：总销售额、销量、趋势）',
                                           property_analysis STRING COMMENT '属性分析（JSON格式：有效属性、转化数据）',
                                           traffic_effect STRING COMMENT '流量效果（JSON格式：渠道占比、热搜词）',
                                           user_insight STRING COMMENT '人群洞察（JSON格式：核心标签、行为特征）',
                                           stat_date STRING COMMENT '统计日期'
)
    PARTITIONED BY (dt STRING COMMENT '分区日期')
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS ORC;

-- 关闭统计信息自动收集，避免执行报错
SET hive.stats.autogather=false;

-- 从DWS层聚合数据至ADS层品类360全景表（分区dt='2025-01-25'）
INSERT INTO TABLE ads_category_360_overview PARTITION (dt='2025-01-25')
SELECT
    s.category_id,
    -- 品类名称（实际场景需关联商品维度表）
    '品类名称占位' AS category_name,
    -- 销售概况JSON（总销售额、销量、趋势等）
    CONCAT(
            '{"total_sales_amount":', s.total_sales_amount,
            ',"total_sales_num":', s.total_sales_num,
            ',"sales_trend":[', sales_trend_str, ']',  -- 使用转换后的字符串
            ',"monthly_contribution":', s.monthly_sales_contribution,
            ',"monthly_rank":', s.monthly_rank, '}'
    ) AS sales_overview,
    -- 属性分析JSON（有效属性、流量、转化率）
    CONCAT(
            '{"effective_property":"', p.effective_property,
            '","total_traffic":', p.total_traffic,
            ',"avg_conversion_rate":', p.avg_conversion_rate, '}'
    ) AS property_analysis,
    -- 流量效果JSON（渠道分布、热搜词、转化效果）
    CONCAT(
            '{"channel_distribution":{',
            concat_ws(',', collect_list(concat('"', t.channel_key, '":', t.channel_val))),
            '},"top_search_words":["', concat_ws('","', t.top_search_words), '"]',
            ',"channel_effectiveness":{',
            concat_ws(',', collect_list(concat('"', t.effect_key, '":', t.effect_val))),
            '}}'
    ) AS traffic_effect,
    -- 人群洞察JSON（核心标签、行为、画像）
    CONCAT(
            '{"core_user_tags":["', concat_ws('","', u.core_user_tags), '"]',
            ',"user_behavior":"', u.user_behavior,
            '","user_portrait":{',
            concat_ws(',', collect_list(concat('"', u.portrait_key, '":', u.portrait_val))),
            '}}'
    ) AS user_insight,
    s.stat_date
FROM (
         -- 子查询：将array<double>转换为字符串
         SELECT
             *,
             -- 遍历数组元素并转换为字符串后拼接
             concat_ws(',',
                       cast(sales_trend[0] as string),
                       cast(sales_trend[1] as string),
                       cast(sales_trend[2] as string),
                       cast(sales_trend[3] as string),
                       cast(sales_trend[4] as string)
                 -- 若数组长度超过5，需继续扩展，或使用UDTF动态处理
             ) AS sales_trend_str
         FROM dws_category_sales_summary
         WHERE dt = '2025-01-25' AND stat_date = '2025-01-25'
     ) s
-- 关联属性表
         LEFT JOIN dws_category_property_summary p
                   ON s.category_id = p.category_id
                       AND s.stat_date = p.stat_date
                       AND s.dt = p.dt
-- 关联流量表（拆分map为键值对）
         LEFT JOIN (
    SELECT
        category_id, stat_date, dt, top_search_words,
        channel_key, channel_distribution[channel_key] AS channel_val,
        effect_key, channel_effectiveness[effect_key] AS effect_val
    FROM dws_category_traffic_summary
             LATERAL VIEW explode(map_keys(channel_distribution)) kv1 AS channel_key
             LATERAL VIEW explode(map_keys(channel_effectiveness)) kv2 AS effect_key
    WHERE dt = '2025-01-25' AND stat_date = '2025-01-25'
) t ON s.category_id = t.category_id
    AND s.stat_date = t.stat_date
    AND s.dt = t.dt
-- 关联人群表（拆分map为键值对）
         LEFT JOIN (
    SELECT
        category_id, stat_date, dt, user_behavior, core_user_tags,
        portrait_key, user_portrait[portrait_key] AS portrait_val
    FROM dws_category_user_summary
             LATERAL VIEW explode(map_keys(user_portrait)) kv3 AS portrait_key
    WHERE dt = '2025-01-25' AND stat_date = '2025-01-25'
) u ON s.category_id = u.category_id
    AND s.stat_date = u.stat_date
    AND s.dt = u.dt
GROUP BY
    s.category_id, s.stat_date, s.total_sales_amount, s.total_sales_num,
    s.sales_trend, s.sales_trend_str, s.monthly_sales_contribution, s.monthly_rank,
    p.effective_property, p.total_traffic, p.avg_conversion_rate,
    t.top_search_words, t.channel_key, t.channel_val, t.effect_key, t.effect_val,
    u.core_user_tags, u.user_behavior, u.portrait_key, u.portrait_val;

select * from ads_category_360_overview;

-- todo 2. ads_category_interval_analysis_board（品类区间分析看板）
CREATE TABLE ads_category_strategy_suggest (
                                               category_id STRING COMMENT '品类ID',
                                               operation_suggest STRING COMMENT '运营建议（如渠道优化、属性拓展）',
                                               crowd_targeting STRING COMMENT '人群定位建议',
                                               sales_goal_progress STRING COMMENT '销售目标进度（完成率、差距）',
                                               stat_date STRING COMMENT '统计日期'
)
    PARTITIONED BY (dt STRING COMMENT '分区日期')
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS ORC;

-- 关闭统计信息自动收集，避免执行报错
SET hive.stats.autogather=false;

-- 从DWS层聚合数据至ADS层品类策略建议表（分区dt='2025-01-25'）
INSERT INTO TABLE ads_category_strategy_suggest PARTITION (dt='2025-01-25')
SELECT
    s.category_id,
    -- 运营建议（修正CASE语句格式，补充END和字符串拼接）
    CONCAT(
            '1. 渠道优化：',
            CASE
                WHEN t.channel_effectiveness['手淘搜索'] < 0.1
                    THEN CONCAT('提升手淘搜索转化（当前', t.channel_effectiveness['手淘搜索'], '）；')
                ELSE CONCAT('维持手淘搜索渠道投入（转化良好：', t.channel_effectiveness['手淘搜索'], '）；')
                END,
            '2. 属性拓展：',
            CASE
                WHEN p.avg_conversion_rate < 0.08
                    THEN CONCAT('优化', p.property_name, '属性（平均转化', p.avg_conversion_rate, '）；')
                ELSE CONCAT(p.effective_property, '属性表现优异，可重点推广；')
                END,
            '3. 销售趋势：',
            CASE
                WHEN s.sales_trend[29] < s.sales_trend[28]
                    THEN '近1天销售额下降，需关注库存及活动；'
                ELSE CONCAT('销售额呈上升趋势（+', ROUND((s.sales_trend[29]-s.sales_trend[28])/s.sales_trend[28],2), '%）；')
                END
    ) AS operation_suggest,
    -- 人群定位建议（修正字符串拼接格式）
    CONCAT(
            '核心人群：', concat_ws('+', u.core_user_tags), '；',
            '行为偏好：',
            CASE
                WHEN u.user_behavior = 'search' AND u.user_portrait['20-25岁'] > 0.4
                    THEN '20-25岁搜索人群占比高，建议加强短视频引流；'
                WHEN u.user_behavior = 'pay' AND u.user_portrait['一线城市'] > 0.3
                    THEN '一线城市支付用户突出，可推出高端套餐；'
                ELSE CONCAT('重点触达高转化行为（', u.user_behavior, '）人群；')
                END
    ) AS crowd_targeting,
    -- 销售目标进度（修正计算逻辑的字符串拼接）
    CONCAT(
            '月目标完成率：', ROUND(s.total_sales_amount / (s.last_month_sales * 1.2), 2), '；',
            '差距：', ROUND((s.last_month_sales * 1.2) - s.total_sales_amount, 2), '元；',
            CASE
                WHEN s.total_sales_amount / (s.last_month_sales * 1.2) < 0.7
                    THEN '需加大促销力度；'
                WHEN s.total_sales_amount / (s.last_month_sales * 1.2) < 0.9
                    THEN '冲刺阶段可增加广告投放；'
                ELSE '目标达成良好，维持当前策略；'
                END
    ) AS sales_goal_progress,
    s.stat_date
FROM (
         -- 销售数据及上月销售额（用于目标计算）
         SELECT
             *,
             LAG(total_sales_amount, 30) OVER (PARTITION BY category_id ORDER BY stat_date) AS last_month_sales
         FROM dws_category_sales_summary
         WHERE dt = '2025-01-25' AND stat_date = '2025-01-25'
     ) s
-- 关联属性数据
         LEFT JOIN dws_category_property_summary p
                   ON s.category_id = p.category_id
                       AND s.stat_date = p.stat_date
                       AND s.dt = p.dt
-- 关联流量数据
         LEFT JOIN dws_category_traffic_summary t
                   ON s.category_id = t.category_id
                       AND s.stat_date = t.stat_date
                       AND s.dt = t.dt
-- 关联人群数据（取支付行为人群为主）
         LEFT JOIN (
    SELECT * FROM dws_category_user_summary
    WHERE user_behavior = 'pay'
      AND dt = '2025-01-25'
      AND stat_date = '2025-01-25'
) u
                   ON s.category_id = u.category_id
                       AND s.stat_date = u.stat_date
                       AND s.dt = u.dt;


select * from ads_category_strategy_suggest;



show databases;