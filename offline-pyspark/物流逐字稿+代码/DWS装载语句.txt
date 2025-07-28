use tms;

-- 1.1.1 首日装载
select * from dim_organ_full;
set hive.exec.dynamic.partition.mode=nonstrict; // 可以禁用严格模式 原因:hive默认是开启严格模式
insert overwrite table dws_trade_org_cargo_type_order_1d
    partition (dt)
select org_id,
       org_name,
       city_id,
       region.name city_name,
       cargo_type,
       cargo_type_name,
       order_count,
       order_amount,
       dt
from (select org_id,
             org_name,
             sender_city_id  city_id,
             cargo_type,
             cargo_type_name,
             count(order_id) order_count,
             sum(amount)     order_amount,
             dt
      from (select order_id,
                   cargo_type,
                   cargo_type_name,
                   sender_district_id,
                   sender_city_id,
                   sum(amount) amount,
                   dt
            from (select order_id,
                         cargo_type,
                         cargo_type_name,
                         sender_district_id,
                         sender_city_id,
                         amount,
                         dt
                  from dwd_trade_order_detail_inc) detail
            group by order_id,
                     cargo_type,
                     cargo_type_name,
                     sender_district_id,
                     sender_city_id,
                     dt) distinct_detail
               left join
           (select id org_id,
                   org_name,
                   region_id
            from dim_organ_full
            where dt = '20250719') org
           on distinct_detail.sender_district_id = org.region_id
      group by org_id,
               org_name,
               cargo_type,
               cargo_type_name,
               sender_city_id,
               dt) agg
         left join (
    select id,
           name
    from dim_region_full
    where dt = '20200623'
) region on city_id = region.id;

select * from dws_trade_org_cargo_type_order_1d;

-- 1.1.2 每日装载
select * from dwd_trade_order_detail_inc;
insert overwrite table dws_trade_org_cargo_type_order_1d
    partition (dt = '20250719')
select org_id,
       org_name,
       city_id,
       region.name city_name,
       cargo_type,
       cargo_type_name,
       order_count,
       order_amount
from (select org_id,
             org_name,
             city_id,
             cargo_type,
             cargo_type_name,
             count(order_id) order_count,
             sum(amount)     order_amount
      from (select order_id,
                   cargo_type,
                   cargo_type_name,
                   sender_district_id,
                   sender_city_id city_id,
                   sum(amount)    amount
            from (select order_id,
                         cargo_type,
                         cargo_type_name,
                         sender_district_id,
                         sender_city_id,
                         amount
                  from dwd_trade_order_detail_inc
                  where dt = '20250719') detail
            group by order_id,
                     cargo_type,
                     cargo_type_name,
                     sender_district_id,
                     sender_city_id) distinct_detail
               left join
           (select id org_id,
                   org_name,
                   region_id
            from dim_organ_full
            where dt = '20250719') org
           on distinct_detail.sender_district_id = org.region_id
      group by org_id,
               org_name,
               city_id,
               cargo_type,
               cargo_type_name) agg
         left join (
    select id,
           name
    from dim_region_full
    where dt = '20200623'
) region on city_id = region.id;

select * from dws_trade_org_cargo_type_order_1d;

-- 2.1 dws_trade_org_cargo_type_order_nd
select * from dws_trade_org_cargo_type_order_1d;
insert overwrite table dws_trade_org_cargo_type_order_nd
    partition (dt = '20250719')
select org_id,
       org_name,
       city_id,
       city_name,
       cargo_type,
       cargo_type_name,
       recent_days,
       sum(order_count)  order_count,
       sum(order_amount) order_amount
from dws_trade_org_cargo_type_order_1d lateral view
         explode(array(7, 30)) tmp as recent_days
where dt >= '20250621'
group by org_id,
         org_name,
         city_id,
         city_name,
         cargo_type,
         cargo_type_name,
         recent_days;

select * from dws_trade_org_cargo_type_order_nd;

-- 1.2 dws_trans_org_receive_1d
-- 1.2.1 首日装载
select * from dim_organ_full;

set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dws_trans_org_receive_1d
    partition (dt)
select org_id,
       org_name,
       city_id,
       city_name,
       province_id,
       province_name,
       count(order_id)      order_count,
       sum(distinct_amount) order_amount,
       dt
from (select order_id,
             org_id,
             org_name,
             city_id,
             city_name,
             province_id,
             province_name,
             max(amount) distinct_amount,
             dt
      from (select order_id,
                   amount,
                   sender_district_id,
                   dt
            from dwd_trans_receive_detail_inc) detail
               left join
           (select id org_id,
                   org_name,
                   region_id
            from dim_organ_full
            where dt = '20250719') organ
           on detail.sender_district_id = organ.region_id
               left join
           (select id,
                   parent_id
            from dim_region_full
            where dt = '20200623') district
           on region_id = district.id
               left join
           (select id   city_id,
                   name city_name,
                   parent_id
            from dim_region_full
            where dt = '20200623') city
           on district.parent_id = city_id
               left join
           (select id   province_id,
                   name province_name,
                   parent_id
            from dim_region_full
            where dt = '20200623') province
           on city.parent_id = province_id
      group by order_id,
               org_id,
               org_name,
               city_id,
               city_name,
               province_id,
               province_name,
               dt) distinct_tb
group by org_id,
         org_name,
         city_id,
         city_name,
         province_id,
         province_name,
         dt;

select * from dws_trans_org_receive_1d;
-- 1.2.2 每日装载
select * from dwd_trans_receive_detail_inc;
insert overwrite table dws_trans_org_receive_1d
    partition (dt = '20250719')
select org_id,
       org_name,
       city_id,
       city_name,
       province_id,
       province_name,
       count(order_id)      order_count,
       sum(distinct_amount) order_amount
from (select order_id,
             org_id,
             org_name,
             city_id,
             city_name,
             province_id,
             province_name,
             max(amount) distinct_amount
      from (select order_id,
                   amount,
                   sender_district_id
            from dwd_trans_receive_detail_inc
            where dt = '20190610') detail
               left join
           (select id org_id,
                   org_name,
                   region_id
            from dim_organ_full
            where dt = '20250719') organ
           on detail.sender_district_id = organ.region_id
               left join
           (select id,
                   parent_id
            from dim_region_full
            where dt = '20200623') district
           on region_id = district.id
               left join
           (select id   city_id,
                   name city_name,
                   parent_id
            from dim_region_full
            where dt = '20200623') city
           on district.parent_id = city_id
               left join
           (select id   province_id,
                   name province_name,
                   parent_id
            from dim_region_full
            where dt = '20200623') province
           on city.parent_id = province_id
      group by order_id,
               org_id,
               org_name,
               city_id,
               city_name,
               province_id,
               province_name) distinct_tb
group by org_id,
         org_name,
         city_id,
         city_name,
         province_id,
         province_name;

select * from dws_trans_org_receive_1d;


-- 2.2 dws_trans_org_receive_nd
select * from dws_trans_org_receive_1d;
insert overwrite table dws_trans_org_receive_nd
    partition (dt = '20190610')
select org_id,
       org_name,
       city_id,
       city_name,
       province_id,
       province_name,
       recent_days,
       sum(order_count)  order_count,
       sum(order_amount) order_amount
from dws_trans_org_receive_1d
         lateral view explode(array(7, 30)) tmp as recent_days
where dt >= '20190610'
group by org_id,
         org_name,
         city_id,
         city_name,
         province_id,
         province_name,
         recent_days;

select * from dws_trans_org_receive_nd;

-- 1.3 dws_trans_dispatch_1d
-- 1.3.1 首日装载
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dws_trans_dispatch_1d
    partition (dt)
select count(order_id)      order_count,
       sum(distinct_amount) order_amount,
       dt
from (select order_id,
             dt,
             max(amount) distinct_amount
      from dwd_trans_dispatch_detail_inc
      group by order_id,
               dt) distinct_info
group by dt;

select * from dws_trans_dispatch_1d;
-- 1.3.2 每日装载
select * from dwd_trans_dispatch_detail_inc;
insert overwrite table dws_trans_dispatch_1d
    partition (dt = '2025-07-19')
select count(order_id)      order_count,
       sum(distinct_amount) order_amount
from (select order_id,
             max(amount) distinct_amount
      from dwd_trans_dispatch_detail_inc
      where dt = '2025-07-19'
      group by order_id) distinct_info;

-- 2.3 dws_trans_dispatch_nd
select * from dws_trans_dispatch_1d;
SET hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dws_trans_dispatch_nd
    partition (dt = '2025-07-19')
select recent_days,
       sum(order_count)  order_count,
       sum(order_amount) order_amount
from dws_trans_dispatch_1d lateral view
         explode(array(7, 30)) tmp as recent_days
where dt >= '2025-07-19'
group by recent_days;

select * from dws_trans_dispatch_nd;

-- 3.1 dws_trans_dispatch_td
-- 3.1.1 首日装载
select * from dws_trans_dispatch_1d;
insert overwrite table dws_trans_dispatch_td
    partition (dt = '2025-07-19')
select sum(order_count)  order_count,
       sum(order_amount) order_amount
from dws_trans_dispatch_1d;

select * from dws_trans_dispatch_td;
-- 3.1.2 每日装载
select * from dws_trans_dispatch_td;
insert overwrite table dws_trans_dispatch_td
    partition (dt = '2025-07-19')
select sum(order_count)  order_count,
       sum(order_amount) order_amount
from (select order_count,
             order_amount
      from dws_trans_dispatch_td
      where dt = date_add('2025-07-20', -1)
      union
      select order_count,
             order_amount
      from dws_trans_dispatch_1d
      where dt = '2025-07-19') all_data;

select * from dws_trans_dispatch_td;
-- 3.2 dws_trans_bound_finish_td
-- 3.2.1 首日装载
select *  from dwd_trans_bound_finish_detail_inc;
insert overwrite table dws_trans_bound_finish_td
    partition (dt = '2025-07-19')
select count(order_id)   order_count,
       sum(order_amount) order_amount
from (select order_id,
             max(amount) order_amount
      from dwd_trans_bound_finish_detail_inc
      group by order_id) distinct_info;

select * from dws_trans_bound_finish_td;
-- 3.2.2 每日装载
select * from dws_trans_bound_finish_td;
insert overwrite table dws_trans_bound_finish_td
    partition (dt = '2025-07-19')
select sum(order_count)  order_count,
       sum(order_amount) order_amount
from (select order_count,
             order_amount
      from dws_trans_bound_finish_td
      where dt = date_add('2025-07-20', -1)
      union
      select count(order_id)   order_count,
             sum(order_amount) order_amount
      from (select order_id,
                   max(amount) order_amount
            from dwd_trans_bound_finish_detail_inc
            where dt = '2025-07-19'
            group by order_id) distinct_tb) all_data;

select * from dws_trans_bound_finish_td;
-- 1.4 dws_trans_org_truck_type_trans_finish_1d
-- 1.4.1 首日装载
select * from dim_truck_full;
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dws_trans_org_truck_model_type_trans_finish_1d
    partition (dt)
select org_id,
       org_name,
       truck_model_type,
       truck_model_type_name,
       count(trans_finish.id) truck_finish_count,
       sum(actual_distance)   trans_finish_distance,
       sum(finish_dur_sec)    finish_dur_sec,
       dt
from (select id,
             start_org_id   org_id,
             start_org_name org_name,
             truck_id,
             actual_distance,
             finish_dur_sec,
             dt
      from dwd_trans_trans_finish_inc) trans_finish
         left join
     (select id,
             truck_model_type,
             truck_model_type_name
      from dim_truck_full
      where dt = '20250719') truck_info
     on trans_finish.truck_id = truck_info.id
group by org_id,
         org_name,
         truck_model_type,
         truck_model_type_name,
         dt;

select * from dws_trans_org_truck_model_type_trans_finish_1d;
-- 1.4.2 每日装载
select * from dim_truck_full;
insert overwrite table dws_trans_org_truck_model_type_trans_finish_1d
    partition (dt = '20250719')
select org_id,
       org_name,
       truck_model_type,
       truck_model_type_name,
       count(trans_finish.id) truck_finish_count,
       sum(actual_distance)   trans_finish_distance,
       sum(finish_dur_sec)    finish_dur_sec
from (select id,
             start_org_id   org_id,
             start_org_name org_name,
             truck_id,
             actual_distance,
             finish_dur_sec
      from dwd_trans_trans_finish_inc
      where dt = '20250719') trans_finish
         left join
     (select id,
             truck_model_type,
             truck_model_type_name
      from dim_truck_full
      where dt = '20250719') truck_info
     on trans_finish.truck_id = truck_info.id
group by org_id,
         org_name,
         truck_model_type,
         truck_model_type_name;

select * from dws_trans_org_truck_model_type_trans_finish_1d;
-- 2.4 dws_trans_shift_trans_finish_nd
select * from dws_trans_shift_trans_finish_nd;
insert overwrite table dws_trans_shift_trans_finish_nd
    partition (dt = '20250719')
select shift_id,
       if(org_level = 1, first.region_id, city.id)     city_id,
       if(org_level = 1, first.region_name, city.name) city_name,
       org_id,
       org_name,
       line_id,
       line_name,
       driver1_emp_id,
       driver1_name,
       driver2_emp_id,
       driver2_name,
       truck_model_type,
       truck_model_type_name,
       recent_days,
       trans_finish_count,
       trans_finish_distance,
       trans_finish_dur_sec,
       trans_finish_order_count,
       trans_finish_delay_count
from (select recent_days,
             shift_id,
             line_id,
             truck_id,
             start_org_id                                       org_id,
             start_org_name                                     org_name,
             driver1_emp_id,
             driver1_name,
             driver2_emp_id,
             driver2_name,
             count(id)                                          trans_finish_count,
             sum(actual_distance)                               trans_finish_distance,
             sum(finish_dur_sec)                                trans_finish_dur_sec,
             sum(order_num)                                     trans_finish_order_count,
             sum(if(actual_end_time > actual_distance, 1, 0)) trans_finish_delay_count
      from dwd_trans_trans_finish_inc lateral view
               explode(array(7, 30)) tmp as recent_days
      where dt >= '20250719'
      group by recent_days,
               shift_id,
               line_id,
               start_org_id,
               start_org_name,
               driver1_emp_id,
               driver1_name,
               driver2_emp_id,
               driver2_name,
               truck_id) aggregated
         left join
     (select id,
             org_level,
             region_id,
             region_name
      from dim_organ_full
      where dt = '20250719'
     ) first
     on aggregated.org_id = first.id
         left join
     (select id,
             parent_id
      from dim_region_full
      where dt = '20200623'
     ) parent
     on first.region_id = parent.id
         left join
     (select id,
             name
      from dim_region_full
      where dt = '20200623'
     ) city
     on parent.parent_id = city.id
         left join
     (select id,
             line_name
      from dim_shift_full
      where dt = '20250719') for_line_name
     on shift_id = for_line_name.id
         left join (
    select id,
           truck_model_type,
           truck_model_type_name
    from dim_truck_full
    where dt = '20250719'
) truck_info on truck_id = truck_info.id;

select * from dws_trans_shift_trans_finish_nd;
-- 1.5 dws_trans_org_deliver_suc_1d
-- 1.5.1 首日装载
select * from dim_organ_full;
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dws_trans_org_deliver_suc_1d
    partition (dt)
select org_id,
       org_name,
       city_id,
       city.name       city_name,
       province_id,
       province.name   province_name,
       count(order_id) order_count,
       dt
from (select order_id,
             sender_district_id,
             dt
      from dwd_trans_deliver_suc_detail_inc
      group by order_id, sender_district_id, dt) detail
         left join
     (select id org_id,
             org_name,
             region_id district_id
      from dim_organ_full
      where dt = '20250719') organ
     on detail.sender_district_id = organ.district_id
         left join
     (select id,
             parent_id city_id
      from dim_region_full
      where dt = '20200623') district
     on district_id = district.id
         left join
     (select id,
             name,
             parent_id province_id
      from dim_region_full
      where dt = '20200623') city
     on city_id = city.id
         left join
     (select id,
             name
      from dim_region_full
      where dt = '20200623') province
     on province_id = province.id
group by org_id,
         org_name,
         city_id,
         city.name,
         province_id,
         province.name,
         dt;
select * from dws_trans_org_deliver_suc_1d;
-- 1.5.2 每日装载
insert overwrite table dws_trans_org_deliver_suc_1d
    partition (dt = '2023-01-11')
select org_id,
       org_name,
       city_id,
       city.name       city_name,
       province_id,
       province.name   province_name,
       count(order_id) order_count
from (select order_id,
             sender_district_id
      from dwd_trans_deliver_suc_detail_inc
      where dt = '2023-01-11'
      group by order_id, sender_district_id) detail
         left join
     (select id org_id,
             org_name,
             region_id district_id
      from dim_organ_full
      where dt = '2023-01-11') organ
     on detail.sender_district_id = organ.district_id
         left join
     (select id,
             parent_id city_id
      from dim_region_full
      where dt = '2023-01-11') district
     on district_id = district.id
         left join
     (select id,
             name,
             parent_id province_id
      from dim_region_full
      where dt = '2023-01-11') city
     on city_id = city.id
         left join
     (select id,
             name
      from dim_region_full
      where dt = '2023-01-11') province
     on province_id = province.id
group by org_id,
         org_name,
         city_id,
         city.name,
         province_id,
         province.name;

-- 2.5 dws_trans_org_deliver_suc_nd
select * from dws_trans_org_deliver_suc_1d;
insert overwrite table dws_trans_org_deliver_suc_nd
    partition (dt = '20220708')
select org_id,
       org_name,
       city_id,
       city_name,
       province_id,
       province_name,
       recent_days,
       sum(order_count) order_count
from dws_trans_org_deliver_suc_1d lateral view
         explode(array(7, 30)) tmp as recent_days
where dt >= '20220708'
group by org_id,
         org_name,
         city_id,
         city_name,
         province_id,
         province_name,
         recent_days;
select * from dws_trans_org_deliver_suc_nd;
-- 1.6 dws_trans_org_sort_1d
-- 1.6.1 首日装载
set hive.exec.dynamic.partition.mode=nonstrict;
select * from dim_organ_full;
insert overwrite table dws_trans_org_sort_1d
    partition (dt)
select org_id,
       org_name,
       if(org_level = 1, city_for_level1.id, province_for_level1.id)         city_id,
       if(org_level = 1, city_for_level1.name, province_for_level1.name)     city_name,
       if(org_level = 1, province_for_level1.id, province_for_level2.id)     province_id,
       if(org_level = 1, province_for_level1.name, province_for_level2.name) province_name,
       sort_count,
       dt
from (select org_id,
             count(*) sort_count,
             dt
      from dwd_bound_sort_inc
      group by org_id, dt) agg
         left join
     (select id,
             org_name,
             org_level,
             region_id
      from dim_organ_full
      where dt = '20250719') org
     on org_id = org.id
         left join
     (select id,
             name,
             parent_id
      from dim_region_full
      where dt = '20200623') city_for_level1
     on region_id = city_for_level1.id
         left join
     (select id,
             name,
             parent_id
      from dim_region_full
      where dt = '20200623') province_for_level1
     on city_for_level1.parent_id = province_for_level1.id
         left join
     (select id,
             name,
             parent_id
      from dim_region_full
      where dt = '20200623') province_for_level2
     on province_for_level1.parent_id = province_for_level2.id;

select * from dws_trans_org_sort_1d;

-- 1.6.2 每日装载
insert overwrite table dws_trans_org_sort_1d
    partition (dt = '2023-01-11')
select org_id,
       org_name,
       if(org_level = 1, city_for_level1.id, province_for_level1.id)         city_id,
       if(org_level = 1, city_for_level1.name, province_for_level1.name)     city_name,
       if(org_level = 1, province_for_level1.id, province_for_level2.id)     province_id,
       if(org_level = 1, province_for_level1.name, province_for_level2.name) province_name,
       sort_count
from (select org_id,
             count(*) sort_count
      from dwd_bound_sort_inc
      where dt = '2023-01-11'
      group by org_id) agg
         left join
     (select id,
             org_name,
             org_level,
             region_id
      from dim_organ_full
      where dt = '2023-01-11') org
     on org_id = org.id
         left join
     (select id,
             name,
             parent_id
      from dim_region_full
      where dt = '2023-01-11') city_for_level1
     on region_id = city_for_level1.id
         left join
     (select id,
             name,
             parent_id
      from dim_region_full
      where dt = '2023-01-11') province_for_level1
     on city_for_level1.parent_id = province_for_level1.id
         left join
     (select id,
             name,
             parent_id
      from dim_region_full
      where dt = '2023-01-11') province_for_level2
     on province_for_level1.parent_id = province_for_level2.id;

-- 2.6 dws_trans_org_sort_nd
set hive.exec.dynamic.partition.mode=nonstrict;

select * from dws_trans_org_sort_1d;
insert overwrite table dws_trans_org_sort_nd
    partition (dt = '20250719')
select org_id,
       org_name,
       city_id,
       city_name,
       province_id,
       province_name,
       recent_days,
       sum(sort_count) sort_count
from dws_trans_org_sort_1d lateral view
         explode(array(7, 30)) tmp as recent_days
where dt >= '20250719'
group by org_id,
         org_name,
         city_id,
         city_name,
         province_id,
         province_name,
         recent_days;

select * from dws_trans_org_sort_nd;