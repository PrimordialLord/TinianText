use tms;

-- 最近 1/7/30 日下单数
-- 最近 1/7/30 日下单金额
-- 最近 1/7/30 日各类型货物下单数
-- 最近 1/7/30 日各类型货物下单金额
-- 最近 1/7/30 日各城市下单数
-- 最近 1/7/30 日各城市下单金额
-- 最近 1/7/30 日各机构下单数
-- 最近 1/7/30 日各机构下单金额
-- 被依赖的
-- 最近 1/7/30 日各机构各类型货物下单数
-- 最近 1/7/30 日各机构各类型货物下单金额
-- 1.1 交易域机构货物类型粒度下单 1 日汇总表
drop table if exists dws_trade_org_cargo_type_order_1d;
create external table dws_trade_org_cargo_type_order_1d(
	`org_id` bigint comment '机构ID',
	`org_name` string comment '转运站名称',
	`city_id` bigint comment '城市ID',
	`city_name` string comment '城市名称',
	`cargo_type` string comment '货物类型',
	`cargo_type_name` string comment '货物类型名称',
	`order_count` bigint comment '下单数',
	`order_amount` decimal(16,2) comment '下单金额'
) comment '交易域机构货物类型粒度下单 1 日汇总表'
	partitioned by(`dt` string comment '统计日期')
	stored as orc
	location '/warehouse/tms/dws/dws_trade_org_cargo_type_order_1d'
	tblproperties('orc.compress' = 'snappy');

-- 2.1 交易域机构货物类型粒度下单 n 日汇总表
drop table if exists dws_trade_org_cargo_type_order_nd;
create external table dws_trade_org_cargo_type_order_nd(
	`org_id` bigint comment '机构ID',
	`org_name` string comment '转运站名称',
	`city_id` bigint comment '城市ID',
	`city_name` string comment '城市名称',
	`cargo_type` string comment '货物类型',
	`cargo_type_name` string comment '货物类型名称',
	`recent_days` tinyint comment '最近天数',
	`order_count` bigint comment '下单数',
	`order_amount` decimal(16,2) comment '下单金额'
) comment '交易域机构货物类型粒度下单 n 日汇总表'
	partitioned by(`dt` string comment '统计日期')
	stored as orc
	location '/warehouse/tms/dws/dws_trade_org_cargo_type_order_nd'
	tblproperties('orc.compress' = 'snappy');

-- 最近 1/7/30 日接单总数
-- 最近 1/7/30 日接单金额
-- 最近 1/7/30 日各省份揽收次数
-- 最近 1/7/30 日各省份揽收金额
-- 最近 1/7/30 日各城市揽收次数
-- 最近 1/7/30 日各城市揽收金额
-- 最近 1/7/30 日各转运站揽收次数
-- 最近 1/7/30 日各转运站揽收金额
-- 被依赖的
-- 最近 1/7/30 日各转运站揽收次数

-- 1.2 物流域转运站粒度揽收 1 日汇总表
drop table if exists dws_trans_org_receive_1d;
create external table dws_trans_org_receive_1d(
	`org_id` bigint comment '转运站ID',
	`org_name` string comment '转运站名称',
	`city_id` bigint comment '城市ID',
	`city_name` string comment '城市名称',
	`province_id` bigint comment '省份ID',
	`province_name` string comment '省份名称',
	`order_count` bigint comment '揽收次数',
	`order_amount` decimal(16, 2) comment '揽收金额'
) comment '物流域转运站粒度揽收 1 日汇总表'
	partitioned by (`dt` string comment '统计日期')
	stored as orc
	location '/warehouse/tms/dws/dws_trans_org_receive_1d/'
	tblproperties ('orc.compress'='snappy');

-- 2.2 物流域转运站粒度揽收 n 日汇总表
drop table if exists dws_trans_org_receive_nd;
create external table dws_trans_org_receive_nd(
	`org_id` bigint comment '转运站ID',
	`org_name` string comment '转运站名称',
	`city_id` bigint comment '城市ID',
	`city_name` string comment '城市名称',
	`province_id` bigint comment '省份ID',
	`province_name` string comment '省份名称',
	`recent_days` tinyint comment '最近天数',
	`order_count` bigint comment '揽收次数',
	`order_amount` decimal(16, 2) comment '揽收金额'
) comment '物流域转运站粒度揽收 n 日汇总表'
	partitioned by (`dt` string comment '统计日期')
	stored as orc
	location '/warehouse/tms/dws/dws_trans_org_receive_nd/'
	tblproperties ('orc.compress'='snappy');

-- 最近 1/7/30 日发单总数
-- 最近 1/7/30 日发单金额

-- 1.3 物流域发单 1 日汇总表
drop table if exists dws_trans_dispatch_1d;
create external table dws_trans_dispatch_1d(
	`order_count` bigint comment '发单总数',
	`order_amount` decimal(16,2) comment '发单总金额'
) comment '物流域发单 1 日汇总表'
	partitioned by (`dt` string comment '统计日期')
	stored as orc
	location '/warehouse/tms/dws/dws_trans_dispatch_1d/'
	tblproperties('orc.compress'='snappy');

-- 2.3 物流域发单 n 日汇总表
drop table if exists dws_trans_dispatch_nd;
create external table dws_trans_dispatch_nd(
	`recent_days` tinyint comment '最近天数',
	`order_count` bigint comment '发单总数',
	`order_amount` decimal(16,2) comment '发单总金额'
) comment '物流域发单 1 日汇总表'
	partitioned by (`dt` string comment '统计日期')
	stored as orc
	location '/warehouse/tms/dws/dws_trans_dispatch_nd'
	tblproperties('orc.compress'='snappy');

-- 历史至今运输中运单总数
-- 历史至今运输中运单总金额
-- 被依赖的
-- 历史至今发单数
-- 历史至今发单金额

-- 3.1 物流域发单历史至今汇总表
drop table if exists dws_trans_dispatch_td;
create external table dws_trans_dispatch_td(
	`order_count` bigint comment '发单数',
	`order_amount` decimal(16,2) comment '发单金额'
) comment '物流域发单历史至今汇总表'
	partitioned by (`dt` string comment '统计日期')
	stored as orc
	location '/warehouse/tms/dws/dws_trans_dispatch_td'
	tblproperties('orc.compress'='snappy');

-- 被依赖的
-- 历史至今转运完成运单数
-- 历史至今转运完成运单金额

-- 3.2 物流域转运完成历史至今汇总表
drop table if exists dws_trans_bound_finish_td;
create external table dws_trans_bound_finish_td(
	`order_count` bigint comment '发单数',
	`order_amount` decimal(16,2) comment '发单金额'
) comment '物流域转运完成历史至今汇总表'
	partitioned by (`dt` string comment '统计日期')
	stored as orc
	location '/warehouse/tms/dws/dws_trans_bound_finish_td'
	tblproperties('orc.compress'='snappy');

-- 最近 1 日完成运输次数
-- 最近 1 日完成运输里程
-- 最近 1 日完成运输时长
-- 最近 1 日各城市完成运输次数
-- 最近 1 日各城市完成运输里程
-- 最近 1 日各城市完成运输时长
-- 最近 1 日各机构完成运输次数
-- 最近 1 日各机构完成运输里程
-- 最近 1 日各机构完成运输时长
-- 最近 1 日各类卡车完成运输次数
-- 最近 1 日各类卡车完成运输里程
-- 最近 1 日各类卡车完成运输时长
-- 被依赖的
-- 最近 1 日各机构各类卡车完成运输次数
-- 最近 1 日各机构各类卡车完成运输里程
-- 最近 1 日各机构各类卡车完成运输时长

-- 1.4 物流域机构卡车类别粒度运输最近 1 日汇总表
drop table if exists dws_trans_org_truck_model_type_trans_finish_1d;
create external table dws_trans_org_truck_model_type_trans_finish_1d(
	`org_id` bigint comment '机构ID',
	`org_name` string comment '机构名称',
	`truck_model_type` string comment '卡车类别编码',
	`truck_model_type_name` string comment '卡车类别名称',
	`trans_finish_count` bigint comment '运输完成次数',
	`trans_finish_distance` decimal(16,2) comment '运输完成里程',
	`trans_finish_dur_sec` bigint comment '运输完成时长，单位：秒'
) comment '物流域机构卡车类别粒度运输最近 1 日汇总表'
	partitioned by (`dt` string comment '统计日期')
	stored as orc
	location '/warehouse/tms/dws/dws_trans_org_truck_model_type_trans_finish_1d/'
	tblproperties('orc.compress'='snappy');

-- 派生
-- 最近 7/30 日各城市完成运输次数
-- 最近 7/30 日各城市完成运输里程
-- 最近 7/30 日各城市完成运输时长
-- 最近 7/30 日各机构完成运输次数
-- 最近 7/30 日各机构完成运输里程
-- 最近 7/30 日各机构完成运输时长
-- 最近 7/30 日各线路完成运输次数
-- 最近 7/30 日各线路完成运输里程
-- 最近 7/30 日各线路总运输时长
-- 最近 7/30 日各线路运输完成运单数
-- 最近 7/30 日各司机运输次数
-- 最近 7/30 日各司机运输里程
-- 最近 7/30 日各司机总运输时长
-- 最近 7/30 日各司机逾期次数
-- 最近 7/30 日各类卡车完成运输次数
-- 最近 7/30 日各类卡车完成运输里程
-- 最近 7/30 日各类卡车完成运输时长

-- 衍生
-- 最近 7/30 日各司机平均每次运输时长
-- 最近 7/30 日各司机平均每次运输里程
-- 最近 1/7/30 日各类卡车平均每次运输时长
-- 最近 1/7/30 日各类卡车平均每次运输里程

-- 被依赖的
-- 最近 7/30 日各班次完成运输次数
-- 最近 7/30 日各班次总运输里程
-- 最近 7/30 日各班次总运输时长
-- 最近 7/30 日各班次运输完成运单数
-- 最近 7/30 日各班次逾期次数

-- 2.4 物流域班次粒度转运完成最近 n 日汇总表
drop table if exists dws_trans_shift_trans_finish_nd;
create external table dws_trans_shift_trans_finish_nd(
	`shift_id` bigint comment '班次ID',
	`city_id` bigint comment '城市ID',
	`city_name` string comment '城市名称',
	`org_id` bigint comment '机构ID',
	`org_name` string comment '机构名称',
	`line_id` bigint comment '线路ID',
	`line_name` string comment '线路名称',
	`driver1_emp_id` bigint comment '第一司机员工ID',
	`driver1_name` string comment '第一司机姓名',
	`driver2_emp_id` bigint comment '第二司机员工ID',
	`driver2_name` string comment '第二司机姓名',
	`truck_model_type` string comment '卡车类别编码',
	`truck_model_type_name` string comment '卡车类别名称',
	`recent_days` tinyint comment '最近天数',
	`trans_finish_count` bigint comment '转运完成次数',
	`trans_finish_distance` decimal(16,2) comment '转运完成里程',
	`trans_finish_dur_sec` bigint comment '转运完成时长，单位：秒',
	`trans_finish_order_count` bigint comment '转运完成运单数',
	`trans_finish_delay_count` bigint comment '逾期次数'
) comment '物流域班次粒度转运完成最近 n 日汇总表'
	partitioned by (`dt` string comment '统计日期')
	stored as orc
	location '/warehouse/tms/dws/dws_trans_shift_trans_finish_nd/'
	tblproperties('orc.compress'='snappy');

-- 最近 1/7/30 日派送成功次数
-- 最近 1/7/30 日各省份派送成功次数
-- 最近 1/7/30 日各城市派送成功次数

-- 被依赖的
-- 最近 1/7/30 日各转运站派送成功次数

-- 1.5 物流域转运站粒度派送成功 1 日汇总表
drop table if exists dws_trans_org_deliver_suc_1d;
create external table dws_trans_org_deliver_suc_1d(
	`org_id` bigint comment '转运站ID',
	`org_name` string comment '转运站名称',
	`city_id` bigint comment '城市ID',
	`city_name` string comment '城市名称',
	`province_id` bigint comment '省份ID',
	`province_name` string comment '省份名称',
	`order_count` bigint comment '派送成功次数（订单数）'
) comment '物流域转运站粒度派送成功 1 日汇总表'
	partitioned by (`dt` string comment '统计日期')
	stored as orc
	location '/warehouse/tms/dws/dws_trans_org_deliver_suc_1d/'
	tblproperties('orc.compress'='snappy');

-- 2.5 物流域转运站粒度派送成功 n 日汇总表
drop table if exists dws_trans_org_deliver_suc_nd;
create external table dws_trans_org_deliver_suc_nd(
	`org_id` bigint comment '转运站ID',
	`org_name` string comment '转运站名称',
	`city_id` bigint comment '城市ID',
	`city_name` string comment '城市名称',
	`province_id` bigint comment '省份ID',
	`province_name` string comment '省份名称',
	`recent_days` tinyint comment '最近天数',
	`order_count` bigint comment '派送成功次数（订单数）'
) comment '物流域转运站粒度派送成功 n 日汇总表'
	partitioned by (`dt` string comment '统计日期')
	stored as orc
	location '/warehouse/tms/dws/dws_trans_org_deliver_suc_nd/'
	tblproperties('orc.compress'='snappy');

-- 最近 1/7/30 日分拣次数
-- 最近 1/7/30 日各省份分拣次数
-- 最近 1/7/30 日各城市分拣次数
-- 被依赖的
-- 最近 1/7/30 日各机构分拣次数

-- 1.6 物流域机构粒度分拣 1 日汇总表
drop table if exists dws_trans_org_sort_1d;
create external table dws_trans_org_sort_1d(
	`org_id` bigint comment '机构ID',
	`org_name` string comment '机构名称',
	`city_id` bigint comment '城市ID',
	`city_name` string comment '城市名称',
	`province_id` bigint comment '省份ID',
	`province_name` string comment '省份名称',
	`sort_count` bigint comment '分拣次数'
) comment '物流域机构粒度分拣 1 日汇总表'
	partitioned by (`dt` string comment '统计日期')
	stored as orc
	location '/warehouse/tms/dws/dws_trans_org_sort_1d/'
	tblproperties('orc.compress'='snappy');

-- 2.6 物流域机构粒度分拣 n 日汇总表
drop table if exists dws_trans_org_sort_nd;
create external table dws_trans_org_sort_nd(
	`org_id` bigint comment '机构ID',
	`org_name` string comment '机构名称',
	`city_id` bigint comment '城市ID',
	`city_name` string comment '城市名称',
	`province_id` bigint comment '省份ID',
	`province_name` string comment '省份名称',
	`recent_days` tinyint comment '最近天数',
	`sort_count` bigint comment '分拣次数'
) comment '物流域机构粒度分拣 n 日汇总表'
	partitioned by (`dt` string comment '统计日期')
	stored as orc
	location '/warehouse/tms/dws/dws_trans_org_sort_nd/'
	tblproperties('orc.compress'='snappy');