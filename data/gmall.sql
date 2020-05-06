ods
===========================
------日志数据--------
===========================
ods_start_log
===========================
建表语句
===========================
drop table if exists ods_start_log;
CREATE EXTERNAL TABLE ods_start_log (`line` string)
PARTITIONED BY (`dt` string)
STORED AS
  INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION '/warehouse/gmall/ods/ods_start_log';
===========================
数据装载
===========================
load data inpath '/origin_data/gmall/log/topic_start/2020-03-15' into table ods_start_log patition(dt='2020-03-15');
hadoop jar /opt/module/hadoop-2.7.2/share/hadoop/common/hadoop-lzo-0.4.20.jar com.hadoop.compression.lzo.DistributedLzoIndexer /warehouse/gmall/ods/ods_start_log/dt=2020-03-15
===========================

===========================
ods_event_log
===========================
建表语句
===========================
drop table if exists ods_event_log;
CREATE EXTERNAL TABLE ods_event_log(`line` string)
PARTITIONED BY (`dt` string)
STORED AS
  INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION '/warehouse/gmall/ods/ods_event_log';
===========================
数据装载
===========================
load data inpath '/origin_data/gmall/log/topic_event/2020-03-15' into table ods_event_log patition(dt='2020-03-15');
hadoop jar /opt/module/hadoop-2.7.2/share/hadoop/common/hadoop-lzo-0.4.20.jar com.hadoop.compression.lzo.DistributedLzoIndexer /warehouse/gmall/ods/ods_event_log/dt=2020-03-15
===========================
综上 数据装载脚本
===========================
#!/bin/bash
db=gmall
hive=/opt/module/hive/bin/hive
do_date=`date -d '-1 day' +%F`

if [[ -n "$1" ]]; then
	do_date=$1
fi


sql="load data inpath '/origin_data/gmall/log/topic_start/$do_date' into table ${db}.ods_start_log partition(dt='$do_date');
	load data inpath '/origin_data/gmall/log/topic_event/$do_date' into table ${db}.ods_event_log partition(dt='$do_date');
	"
$hive -e "$sql"
hadoop jar /opt/module/hadoop-2.7.2/share/hadoop/common/hadoop-lzo-0.4.20.jar com.hadoop.compression.lzo.DistributedLzoIndexer /warehouse/gmall/ods/ods_start_log/dt=$do_date
hadoop jar /opt/module/hadoop-2.7.2/share/hadoop/common/hadoop-lzo-0.4.20.jar com.hadoop.compression.lzo.DistributedLzoIndexer /warehouse/gmall/ods/ods_event_log/dt=$do_date
===========================
===========================





-----业务数据-------

===========================
1.ods_order_info
===========================
drop table if exists ods_order_info;
create external table ods_order_info (
    `id` string COMMENT '订单编号',
    `final_total_amount` decimal(10,2) COMMENT '订单金额',
    `order_status` string COMMENT '订单状态',
    `user_id` string COMMENT '用户id',
    `out_trade_no` string COMMENT '支付流水号',
    `create_time` string COMMENT '创建时间',
    `operate_time` string COMMENT '创建时间',
    `province_id` string COMMENT '省份ID',
    `benefit_reduce_amount` decimal(10,2) COMMENT '优惠金额',
    `original_total_amount` decimal(10,2)  COMMENT '原价金额',
    `feight_fee` decimal(10,2)  COMMENT '运费'
) COMMENT '订单表'
PARTITIONED BY (`dt` string)
row format delimited fields terminated by '\t'
STORED AS
  INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/warehouse/gmall/ods/ods_order_info/';
===========================
2.ods_order_detail
===========================
drop table if exists ods_order_detail;
create external table ods_order_detail( 
    `id` string COMMENT '订单编号',
    `order_id` string  COMMENT '订单号', 
    `user_id` string COMMENT '用户id',
    `sku_id` string COMMENT '商品id',
    `sku_name` string COMMENT '商品名称',
    `order_price` decimal(10,2) COMMENT '商品价格',
    `sku_num` bigint COMMENT '商品数量',
    `create_time` string COMMENT '创建时间'
) COMMENT '订单明细表'
PARTITIONED BY (`dt` string)
row format delimited fields terminated by '\t' 
STORED AS
  INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/warehouse/gmall/ods/ods_order_detail/';
===========================
3.ods_sku_info
===========================
drop table if exists ods_sku_info;
create external table ods_sku_info( 
    `id` string COMMENT 'skuId',
    `spu_id` string   COMMENT 'spuid', 
    `price` decimal(10,2) COMMENT '价格',
    `sku_name` string COMMENT '商品名称',
    `sku_desc` string COMMENT '商品描述',
    `weight` string COMMENT '重量',
    `tm_id` string COMMENT '品牌id',
    `category3_id` string COMMENT '品类id',
    `create_time` string COMMENT '创建时间'
) COMMENT '商品表'
PARTITIONED BY (`dt` string)
row format delimited fields terminated by '\t'
STORED AS
  INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/warehouse/gmall/ods/ods_sku_info/';
===========================
4.ods_user_info
===========================
drop table if exists ods_user_info;
create external table ods_user_info( 
    `id` string COMMENT '用户id',
    `name`  string COMMENT '姓名',
    `birthday` string COMMENT '生日',
    `gender` string COMMENT '性别',
    `email` string COMMENT '邮箱',
    `user_level` string COMMENT '用户等级',
    `create_time` string COMMENT '创建时间',
    `operate_time` string COMMENT '操作时间'
) COMMENT '用户信息'
PARTITIONED BY (`dt` string)
row format delimited fields terminated by '\t'
STORED AS
  INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/warehouse/gmall/ods/ods_user_info/';
===========================
5.ods_base_category1
===========================
drop table if exists ods_base_category1;
create external table ods_base_category1( 
    `id` string COMMENT 'id',
    `name`  string COMMENT '名称'
) COMMENT '商品一级分类'
PARTITIONED BY (`dt` string)
row format delimited fields terminated by '\t'
STORED AS
  INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/warehouse/gmall/ods/ods_base_category1/';
===========================
6.ods_base_category2
===========================
drop table if exists ods_base_category2;
create external table ods_base_category2( 
    `id` string COMMENT ' id',
    `name` string COMMENT '名称',
    category1_id string COMMENT '一级品类id'
) COMMENT '商品二级分类'
PARTITIONED BY (`dt` string)
row format delimited fields terminated by '\t'
STORED AS
  INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/warehouse/gmall/ods/ods_base_category2/';
===========================
7.ods_base_category3
===========================
drop table if exists ods_base_category3;
create external table ods_base_category3(
    `id` string COMMENT ' id',
    `name`  string COMMENT '名称',
    category2_id string COMMENT '二级品类id'
) COMMENT '商品三级分类'
PARTITIONED BY (`dt` string)
row format delimited fields terminated by '\t'
STORED AS
  INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/warehouse/gmall/ods/ods_base_category3/';
===========================
8.ods_payment_info
===========================
drop table if exists ods_payment_info;
create external table ods_payment_info(
    `id`   bigint COMMENT '编号',
    `out_trade_no`    string COMMENT '对外业务编号',
    `order_id`        string COMMENT '订单编号',
    `user_id`         string COMMENT '用户编号',
    `alipay_trade_no` string COMMENT '支付宝交易流水编号',
    `total_amount`    decimal(16,2) COMMENT '支付金额',
    `subject`         string COMMENT '交易内容',
    `payment_type`    string COMMENT '支付类型',
    `payment_time`    string COMMENT '支付时间'
   )  COMMENT '支付流水表'
PARTITIONED BY (`dt` string)
row format delimited fields terminated by '\t'
STORED AS
  INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/warehouse/gmall/ods/ods_payment_info/';
===========================
9.ods_base_province
===========================
drop table if exists ods_base_province;
create external table ods_base_province (
    `id`   bigint COMMENT '编号',
    `name`        string COMMENT '省份名称',
    `region_id`    string COMMENT '地区ID',
    `area_code`    string COMMENT '地区编码',
    `iso_code` string COMMENT 'iso编码'
   )  COMMENT '省份表'
row format delimited fields terminated by '\t'
STORED AS
  INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/warehouse/gmall/ods/ods_base_province/';
===========================
10.ods_base_region
===========================
drop table if exists ods_base_region;
create external table ods_base_region (
    `id`   bigint COMMENT '编号',
    `region_name`        string COMMENT '地区名称'
   )  COMMENT '地区表'
row format delimited fields terminated by '\t'
STORED AS
  INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/warehouse/gmall/ods/ods_base_region/';
===========================
11.ods_base_trademark
===========================
drop table if exists ods_base_trademark;
create external table ods_base_trademark (
    `tm_id`   bigint COMMENT '编号',
    `tm_name` string COMMENT '品牌名称'
   )  COMMENT '品牌表'
PARTITIONED BY (`dt` string)
row format delimited fields terminated by '\t'
STORED AS
  INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/warehouse/gmall/ods/ods_base_trademark/';
===========================
12.ods_order_status_log
===========================
drop table if exists ods_order_status_log;
create external table ods_order_status_log (
    `id`   bigint COMMENT '编号',
    `order_id` string COMMENT '订单ID',
    `order_status` string COMMENT '订单状态',
    `operate_time` string COMMENT '修改时间'
   )  COMMENT '订单流水表'
PARTITIONED BY (`dt` string)
row format delimited fields terminated by '\t'
STORED AS
  INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/warehouse/gmall/ods/ods_order_status_log/';
===========================
13.ods_spu_info
===========================
drop table if exists ods_spu_info;
create external table ods_spu_info(
    `id` string COMMENT 'spuid',
    `spu_name` string COMMENT 'spu名称',
    `category3_id` string COMMENT '品类id',
    `tm_id` string COMMENT '品牌id'
) COMMENT '商品spu表'
PARTITIONED BY (`dt` string)
row format delimited fields terminated by '\t'
STORED AS
  INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/warehouse/gmall/ods/ods_spu_info/';
===========================
14.ods_comment_info
===========================
drop table if exists ods_comment_info;
create external table ods_comment_info(
    `id` string COMMENT '编号',
    `user_id` string COMMENT '用户ID',
    `sku_id` string COMMENT '商品ID',
    `spu_id` string COMMENT 'spuid',
    `order_id` string COMMENT '订单ID',
    `appraise` string COMMENT '评价',
    `create_time` string COMMENT '评价时间'
) COMMENT '订单评价表'
PARTITIONED BY (`dt` string)
row format delimited fields terminated by '\t'
STORED AS
  INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/warehouse/gmall/ods/ods_comment_info/';
===========================
15.ods_order_refund_info
===========================
drop table if exists ods_order_refund_info;
create external table ods_order_refund_info(
    `id` string COMMENT '编号',
    `user_id` string COMMENT '用户ID',
    `order_id` string COMMENT '订单ID',
    `sku_id` string COMMENT '商品ID',
    `refund_type` string COMMENT '退款类型',
    `refund_num` string COMMENT '退款件数',
    `refund_amount` string COMMENT '退款金额',
    `refund_reason_type` string COMMENT '退款原因类型',
    `create_time` string COMMENT '退款时间'
) COMMENT '退款信息表'
PARTITIONED BY (`dt` string)
row format delimited fields terminated by '\t'
STORED AS
  INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/warehouse/gmall/ods/ods_order_refund_info/';
===========================
16.ods_cart_info
===========================
drop table if exists ods_cart_info;
create external table ods_cart_info(
    `id` string COMMENT '编号',
    `user_id` string  COMMENT '用户id',
    `sku_id` string  COMMENT 'skuid',
    `cart_price` string  COMMENT '放入购物车时价格',
    `sku_num` string  COMMENT '数量',
    `sku_name` string  COMMENT 'sku名称 (冗余)',
    `create_time` string  COMMENT '创建时间',
    `operate_time` string COMMENT '修改时间',
    `is_ordered` string COMMENT '是否已经下单',
    `order_time` string  COMMENT '下单时间'
) COMMENT '购物车信息表'
PARTITIONED BY (`dt` string)
row format delimited fields terminated by '\t'
STORED AS
  INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/warehouse/gmall/ods/ods_cart_info/';
===========================
17.ods_favor_info
===========================
drop table if exists ods_favor_info;
create external table ods_favor_info(
    `id` string COMMENT '编号',
    `user_id` string  COMMENT '用户id',
    `sku_id` string  COMMENT 'skuid',
    `spu_id` string  COMMENT 'spuid',
    `is_cancel` string  COMMENT '是否取消',
    `create_time` string  COMMENT '收藏时间',
    `cancel_time` string  COMMENT '取消时间'
) COMMENT '收藏信息表'
PARTITIONED BY (`dt` string)
row format delimited fields terminated by '\t'
STORED AS
  INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/warehouse/gmall/ods/ods_favor_info/';
===========================
18.ods_coupon_use
===========================
drop table if exists ods_coupon_use;
create external table ods_coupon_use(
    `id` string COMMENT '编号',
    `coupon_id` string  COMMENT '优惠券ID',
    `user_id` string  COMMENT 'skuid',
    `order_id` string  COMMENT 'spuid',
    `coupon_status` string  COMMENT '优惠券状态',
    `get_time` string  COMMENT '领取时间',
    `using_time` string  COMMENT '使用时间(下单)',
    `used_time` string  COMMENT '使用时间(支付)'
) COMMENT '优惠券使用表'
PARTITIONED BY (`dt` string)
row format delimited fields terminated by '\t'
STORED AS
  INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/warehouse/gmall/ods/ods_coupon_use/';
===========================
19.ods_coupon_info
===========================
drop table if exists ods_coupon_info;
create external table ods_coupon_info(
  `id` string COMMENT '购物券编号',
  `coupon_name` string COMMENT '购物券名称',
  `coupon_type` string COMMENT '购物券类型 1 现金券 2 折扣券 3 满减券 4 满件打折券',
  `condition_amount` string COMMENT '满额数',
  `condition_num` string COMMENT '满件数',
  `activity_id` string COMMENT '活动编号',
  `benefit_amount` string COMMENT '减金额',
  `benefit_discount` string COMMENT '折扣',
  `create_time` string COMMENT '创建时间',
  `range_type` string COMMENT '范围类型 1、商品 2、品类 3、品牌',
  `spu_id` string COMMENT '商品id',
  `tm_id` string COMMENT '品牌id',
  `category3_id` string COMMENT '品类id',
  `limit_num` string COMMENT '最多领用次数',
  `operate_time`  string COMMENT '修改时间',
  `expire_time`  string COMMENT '过期时间'
) COMMENT '优惠券信息表'
PARTITIONED BY (`dt` string)
row format delimited fields terminated by '\t'
STORED AS
  INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/warehouse/gmall/ods/ods_coupon_info/';
===========================
20.ods_activity_info
===========================
drop table if exists ods_activity_info;
create external table ods_activity_info(
    `id` string COMMENT '编号',
    `activity_name` string  COMMENT '优惠券ID',
    `activity_type` string  COMMENT 'skuid',
    `start_time` string  COMMENT 'spuid',
    `end_time` string  COMMENT '优惠券状态',
    `create_time` string  COMMENT '领取时间'
) COMMENT '购物车信息表'
PARTITIONED BY (`dt` string)
row format delimited fields terminated by '\t'
STORED AS
  INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/warehouse/gmall/ods/ods_activity_info/';
===========================
21.ods_activity_order
===========================
drop table if exists ods_activity_order;
create external table ods_activity_order(
    `id` string COMMENT '编号',
    `activity_id` string  COMMENT '优惠券ID',
    `order_id` string  COMMENT 'skuid',
    `create_time` string  COMMENT '领取时间'
) COMMENT '活动订单关联表'
PARTITIONED BY (`dt` string)
row format delimited fields terminated by '\t'
STORED AS
  INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/warehouse/gmall/ods/ods_activity_order/';
===========================
综上，装载脚本
===========================
#!/bin/bash

APP=gmall
hive=/opt/module/hive/bin/hive

# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n "$2" ] ;then
    do_date=$2
else 
    do_date=`date -d "-1 day" +%F`
fi

sql1=" 
load data inpath '/origin_data/$APP/db/order_info/$do_date' OVERWRITE into table ${APP}.ods_order_info partition(dt='$do_date');

load data inpath '/origin_data/$APP/db/order_detail/$do_date' OVERWRITE into table ${APP}.ods_order_detail partition(dt='$do_date');

load data inpath '/origin_data/$APP/db/sku_info/$do_date' OVERWRITE into table ${APP}.ods_sku_info partition(dt='$do_date');

load data inpath '/origin_data/$APP/db/user_info/$do_date' OVERWRITE into table ${APP}.ods_user_info partition(dt='$do_date');

load data inpath '/origin_data/$APP/db/payment_info/$do_date' OVERWRITE into table ${APP}.ods_payment_info partition(dt='$do_date');

load data inpath '/origin_data/$APP/db/base_category1/$do_date' OVERWRITE into table ${APP}.ods_base_category1 partition(dt='$do_date');

load data inpath '/origin_data/$APP/db/base_category2/$do_date' OVERWRITE into table ${APP}.ods_base_category2 partition(dt='$do_date');

load data inpath '/origin_data/$APP/db/base_category3/$do_date' OVERWRITE into table ${APP}.ods_base_category3 partition(dt='$do_date'); 

load data inpath '/origin_data/$APP/db/base_province/$do_date' OVERWRITE into table ${APP}.ods_base_province;

load data inpath '/origin_data/$APP/db/base_region/$do_date' OVERWRITE into table ${APP}.ods_base_region;

load data inpath '/origin_data/$APP/db/base_trademark/$do_date' OVERWRITE into table ${APP}.ods_base_trademark partition(dt='$do_date'); 

load data inpath '/origin_data/$APP/db/activity_info/$do_date' OVERWRITE into table ${APP}.ods_activity_info partition(dt='$do_date'); 

load data inpath '/origin_data/$APP/db/activity_order/$do_date' OVERWRITE into table ${APP}.ods_activity_order partition(dt='$do_date'); 

load data inpath '/origin_data/$APP/db/cart_info/$do_date' OVERWRITE into table ${APP}.ods_cart_info partition(dt='$do_date'); 

load data inpath '/origin_data/$APP/db/comment_info/$do_date' OVERWRITE into table ${APP}.ods_comment_info partition(dt='$do_date'); 

load data inpath '/origin_data/$APP/db/coupon_info/$do_date' OVERWRITE into table ${APP}.ods_coupon_info partition(dt='$do_date'); 

load data inpath '/origin_data/$APP/db/coupon_use/$do_date' OVERWRITE into table ${APP}.ods_coupon_use partition(dt='$do_date'); 

load data inpath '/origin_data/$APP/db/favor_info/$do_date' OVERWRITE into table ${APP}.ods_favor_info partition(dt='$do_date'); 

load data inpath '/origin_data/$APP/db/order_refund_info/$do_date' OVERWRITE into table ${APP}.ods_order_refund_info partition(dt='$do_date'); 

load data inpath '/origin_data/$APP/db/order_status_log/$do_date' OVERWRITE into table ${APP}.ods_order_status_log partition(dt='$do_date'); 

load data inpath '/origin_data/$APP/db/spu_info/$do_date' OVERWRITE into table ${APP}.ods_spu_info partition(dt='$do_date'); 

"
sql2=" 
load data inpath '/origin_data/$APP/db/order_info/$do_date' OVERWRITE into table ${APP}.ods_order_info partition(dt='$do_date');

load data inpath '/origin_data/$APP/db/order_detail/$do_date' OVERWRITE into table ${APP}.ods_order_detail partition(dt='$do_date');

load data inpath '/origin_data/$APP/db/sku_info/$do_date' OVERWRITE into table ${APP}.ods_sku_info partition(dt='$do_date');

load data inpath '/origin_data/$APP/db/user_info/$do_date' OVERWRITE into table ${APP}.ods_user_info partition(dt='$do_date');

load data inpath '/origin_data/$APP/db/payment_info/$do_date' OVERWRITE into table ${APP}.ods_payment_info partition(dt='$do_date');

load data inpath '/origin_data/$APP/db/base_category1/$do_date' OVERWRITE into table ${APP}.ods_base_category1 partition(dt='$do_date');

load data inpath '/origin_data/$APP/db/base_category2/$do_date' OVERWRITE into table ${APP}.ods_base_category2 partition(dt='$do_date');

load data inpath '/origin_data/$APP/db/base_category3/$do_date' OVERWRITE into table ${APP}.ods_base_category3 partition(dt='$do_date'); 

load data inpath '/origin_data/$APP/db/base_trademark/$do_date' OVERWRITE into table ${APP}.ods_base_trademark partition(dt='$do_date'); 

load data inpath '/origin_data/$APP/db/activity_info/$do_date' OVERWRITE into table ${APP}.ods_activity_info partition(dt='$do_date'); 

load data inpath '/origin_data/$APP/db/activity_order/$do_date' OVERWRITE into table ${APP}.ods_activity_order partition(dt='$do_date'); 

load data inpath '/origin_data/$APP/db/cart_info/$do_date' OVERWRITE into table ${APP}.ods_cart_info partition(dt='$do_date'); 

load data inpath '/origin_data/$APP/db/comment_info/$do_date' OVERWRITE into table ${APP}.ods_comment_info partition(dt='$do_date'); 

load data inpath '/origin_data/$APP/db/coupon_info/$do_date' OVERWRITE into table ${APP}.ods_coupon_info partition(dt='$do_date'); 

load data inpath '/origin_data/$APP/db/coupon_use/$do_date' OVERWRITE into table ${APP}.ods_coupon_use partition(dt='$do_date'); 

load data inpath '/origin_data/$APP/db/favor_info/$do_date' OVERWRITE into table ${APP}.ods_favor_info partition(dt='$do_date'); 

load data inpath '/origin_data/$APP/db/order_refund_info/$do_date' OVERWRITE into table ${APP}.ods_ order_refund_info partition(dt='$do_date'); 

load data inpath '/origin_data/$APP/db/order_status_log/$do_date' OVERWRITE into table ${APP}.ods_order_status_log partition(dt='$do_date'); 

load data inpath '/origin_data/$APP/db/spu_info/$do_date' OVERWRITE into table ${APP}.ods_ spu_info partition(dt='$do_date'); 
"
case $1 in
    first )
    $hive -e "$sql1"
        ;;
    all )
    $hive -e "$sql2"
        ;; 
esac


==============================================================================================================================================
--------------------------------------------------------DWD层---------------------------------------------------------------------------------
==============================================================================================================================================
----------------------------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------
===========================
启动日志在DWD层建表，对应ODS层的Json
===========================
drop table if exists dwd_start_log;
CREATE EXTERNAL TABLE dwd_start_log(
`mid_id` string,
`user_id` string, 
`version_code` string, 
`version_name` string, 
`lang` string, 
`source` string, 
`os` string, 
`area` string, 
`model` string,
`brand` string, 
`sdk_version` string, 
`gmail` string, 
`height_width` string,  
`app_time` string,
`network` string, 
`lng` string, 
`lat` string, 
`entry` string, 
`open_ad_type` string, 
`action` string, 
`loading_time` string, 
`detail` string, 
`extend1` string
)
PARTITIONED BY (dt string)
stored as parquet
location '/warehouse/gmall/dwd/dwd_start_log/'
TBLPROPERTIES('parquet.compression'='lzo');
===========================
装载数据
===========================
--简单分析 --get_json_object 是hive自带的取json种特定字段的函数  两个参数............
insert overwrite table dwd_start_log
select 
get_json_object(line,'$.os')
from ods_start_log
where dt = '2020-03-15'
limit 10;
===========================
装载数据脚本
===========================
#!/bin/bash

# 定义变量方便修改
APP=gmall
hive=/opt/module/hive/bin/hive

# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n "$1" ] ;then
	do_date=$1
else 
	do_date=`date -d "-1 day" +%F`  
fi 

sql="
set hive.exec.dynamic.partition.mode=nonstrict;

insert overwrite table "$APP".dwd_start_log
PARTITION (dt='$do_date')
select 
    get_json_object(line,'$.mid') mid_id,
    get_json_object(line,'$.uid') user_id,
    get_json_object(line,'$.vc') version_code,
    get_json_object(line,'$.vn') version_name,
    get_json_object(line,'$.l') lang,
    get_json_object(line,'$.sr') source,
    get_json_object(line,'$.os') os,
    get_json_object(line,'$.ar') area,
    get_json_object(line,'$.md') model,
    get_json_object(line,'$.ba') brand,
    get_json_object(line,'$.sv') sdk_version,
    get_json_object(line,'$.g') gmail,
    get_json_object(line,'$.hw') height_width,
    get_json_object(line,'$.t') app_time,
    get_json_object(line,'$.nw') network,
    get_json_object(line,'$.ln') lng,
    get_json_object(line,'$.la') lat,
    get_json_object(line,'$.entry') entry,
    get_json_object(line,'$.open_ad_type') open_ad_type,
    get_json_object(line,'$.action') action,
    get_json_object(line,'$.loading_time') loading_time,
    get_json_object(line,'$.detail') detail,
    get_json_object(line,'$.extend1') extend1
from "$APP".ods_start_log 
where dt='$do_date';
"

$hive -e "$sql"
===========================


事件日志在DWD层建表，对应ODS层的Json，
事件日志需要进行解析，每个事件去对应相对应的表格
===========================
需要建立UDF和UDTF，创建永久函数
===========================
create function base_analizer as 'com.xstudio.gmall.hive.LogUDF' using jar 'hdfs://hadoop105:9000/user/hive/jars/hive-1.0-SNAPSHOT.jar';
create function flat_analizer as 'com.xstudio.gmall.hive.LogUDTF' using jar 'hdfs://hadoop105:9000/user/hive/jars/hive-1.0-SNAPSHOT.jar'; 
===========================
分析
line1 [1 2 3]
line2 [4 5 6]
line3 [7 8 9]
炸开后是
line event_name event_json
line1 type1 json1
line1 type2 json2
line1 type3 json3
line2 type1 json1
line2 type2 json2
line2 type3 json3
line3 type1 json1
line3 type2 json2
line3 type3 json3
lateral view flat_analizer(base_analizer(line,'et')) tmp as event_name,event_json

--测试用例
select 
*,
event_name,
event_json
from ods_event_log lateral view flat_analizer(base_analizer(line,'et')) tmp as event_name,event_json
where dt = '2020-03-15'
limit 10;
===========================
建立中间表，用来存放炸开后的数据
===========================
drop table if exists dwd_base_event_log;
CREATE EXTERNAL TABLE dwd_base_event_log(
`mid_id` string,
`user_id` string, 
`version_code` string, 
`version_name` string, 
`lang` string, 
`source` string, 
`os` string, 
`area` string, 
`model` string,
`brand` string, 
`sdk_version` string, 
`gmail` string, 
`height_width` string, 
`app_time` string, 
`network` string, 
`lng` string, 
`lat` string, 
`event_name` string, 
`event_json` string, 
`server_time` string)
PARTITIONED BY (`dt` string)
stored as parquet
location '/warehouse/gmall/dwd/dwd_base_event_log/'
TBLPROPERTIES('parquet.compression'='lzo');
===========================
装载数据测试
===========================
insert overwrite table dwd_base_event_log partition(dt='2020-03-15')
select 
base_analizer(line,'mid'),
base_analizer(line,'sv'),
base_analizer(line,'os'),
event_name,
event_json,
base_analizer(line,'st')
from ods_event_log lateral view flat_analizer(base_analizer(line,'et')) tmp as event_name,event_json;	
===========================
装载数据
===========================
insert overwrite table dwd_base_event_log partition(dt='2020-03-15')
select
    base_analizer(line,'mid') as mid_id,
    base_analizer(line,'uid') as user_id,
    base_analizer(line,'vc') as version_code,
    base_analizer(line,'vn') as version_name,
    base_analizer(line,'l') as lang,
    base_analizer(line,'sr') as source,
    base_analizer(line,'os') as os,
    base_analizer(line,'ar') as area,
    base_analizer(line,'md') as model,
    base_analizer(line,'ba') as brand,
    base_analizer(line,'sv') as sdk_version,
    base_analizer(line,'g') as gmail,
    base_analizer(line,'hw') as height_width,
    base_analizer(line,'t') as app_time,
    base_analizer(line,'nw') as network,
    base_analizer(line,'ln') as lng,
    base_analizer(line,'la') as lat,
    event_name,
    event_json,
    base_analizer(line,'st') as server_time
from ods_event_log lateral view flat_analizer(base_analizer(line,'et')) tmp_flat as event_name,event_json
where dt='2020-03-15' and base_analizer(line,'et')<>'';
===========================
通过上述炸开的中间表，为每个事件建表,并且按照事件装载表
===========================
===========================
1.display建表和装载
===========================
drop table if exists dwd_display_log;
CREATE EXTERNAL TABLE dwd_display_log(
`mid_id` string,
`user_id` string,
`version_code` string,
`version_name` string,
`lang` string,
`source` string,
`os` string,
`area` string,
`model` string,
`brand` string,
`sdk_version` string,
`gmail` string,
`height_width` string,
`app_time` string,
`network` string,
`lng` string,
`lat` string,
`action` string,
`goodsid` string,
`place` string,
`extend1` string,
`category` string,
`server_time` string
)
PARTITIONED BY (dt string)
stored as parquet
location '/warehouse/gmall/dwd/dwd_display_log/'
TBLPROPERTIES('parquet.compression'='lzo');

--装载数据
set hive.exec.dynamic.partition.mode=nonstrict;

insert overwrite table dwd_display_log
PARTITION (dt='2020-03-15')
select 
mid_id,
user_id,
version_code,
version_name,
lang,
source,
os,
area,
model,
brand,
sdk_version,
gmail,
height_width,
app_time,
network,
lng,
lat,
get_json_object(event_json,'$.kv.action') action,
get_json_object(event_json,'$.kv.goodsid') goodsid,
get_json_object(event_json,'$.kv.place') place,
get_json_object(event_json,'$.kv.extend1') extend1,
get_json_object(event_json,'$.kv.category') category,
server_time
from dwd_base_event_log 
where dt='2020-03-15' and event_name='display';
===========================
2.dwd_newsdetail_log
===========================
drop table if exists dwd_newsdetail_log;
CREATE EXTERNAL TABLE dwd_newsdetail_log(
`mid_id` string,
`user_id` string, 
`version_code` string, 
`version_name` string, 
`lang` string, 
`source` string, 
`os` string, 
`area` string, 
`model` string,
`brand` string, 
`sdk_version` string, 
`gmail` string, 
`height_width` string, 
`app_time` string,  
`network` string, 
`lng` string, 
`lat` string, 
`entry` string,
`action` string,
`goodsid` string,
`showtype` string,
`news_staytime` string,
`loading_time` string,
`type1` string,
`category` string,
`server_time` string)
PARTITIONED BY (dt string)
stored as parquet
location '/warehouse/gmall/dwd/dwd_newsdetail_log/'
TBLPROPERTIES('parquet.compression'='lzo');

-------装载数据
set hive.exec.dynamic.partition.mode=nonstrict;

insert overwrite table dwd_newsdetail_log
PARTITION (dt='2020-03-15')
select 
mid_id,
user_id,
version_code,
version_name,
lang,
source,
os,
area,
model,
brand,
sdk_version,
gmail,
height_width,
app_time,
network,
lng,
lat,
get_json_object(event_json,'$.kv.entry') entry,
get_json_object(event_json,'$.kv.action') action,
get_json_object(event_json,'$.kv.goodsid') goodsid,
get_json_object(event_json,'$.kv.showtype') showtype,
get_json_object(event_json,'$.kv.news_staytime') news_staytime,
get_json_object(event_json,'$.kv.loading_time') loading_time,
get_json_object(event_json,'$.kv.type1') type1,
get_json_object(event_json,'$.kv.category') category,
server_time
from dwd_base_event_log
where dt='2020-03-15' and event_name='newsdetail';
===========================
3.dwd_loading_log
===========================
drop table if exists dwd_loading_log;
CREATE EXTERNAL TABLE dwd_loading_log(
`mid_id` string,
`user_id` string, 
`version_code` string, 
`version_name` string, 
`lang` string, 
`source` string, 
`os` string, 
`area` string, 
`model` string,
`brand` string, 
`sdk_version` string, 
`gmail` string,
`height_width` string,  
`app_time` string,
`network` string, 
`lng` string, 
`lat` string, 
`action` string,
`loading_time` string,
`loading_way` string,
`extend1` string,
`extend2` string,
`type` string,
`type1` string,
`server_time` string)
PARTITIONED BY (dt string)
stored as parquet
location '/warehouse/gmall/dwd/dwd_loading_log/'
TBLPROPERTIES('parquet.compression'='lzo');

---装载
set hive.exec.dynamic.partition.mode=nonstrict;

insert overwrite table dwd_loading_log
PARTITION (dt='2020-03-15')
select 
mid_id,
user_id,
version_code,
version_name,
lang,
source,
os,
area,
model,
brand,
sdk_version,
gmail,
height_width,
app_time,
network,
lng,
lat,
get_json_object(event_json,'$.kv.action') action,
get_json_object(event_json,'$.kv.loading_time') loading_time,
get_json_object(event_json,'$.kv.loading_way') loading_way,
get_json_object(event_json,'$.kv.extend1') extend1,
get_json_object(event_json,'$.kv.extend2') extend2,
get_json_object(event_json,'$.kv.type') type,
get_json_object(event_json,'$.kv.type1') type1,
server_time
from dwd_base_event_log
where dt='2020-03-15' and event_name='loading';
===========================
4.dwd_ad_log
===========================
drop table if exists dwd_ad_log;
CREATE EXTERNAL TABLE dwd_ad_log(
`mid_id` string,
`user_id` string, 
`version_code` string, 
`version_name` string, 
`lang` string, 
`source` string, 
`os` string, 
`area` string, 
`model` string,
`brand` string, 
`sdk_version` string, 
`gmail` string, 
`height_width` string,  
`app_time` string,
`network` string, 
`lng` string, 
`lat` string, 
`entry` string,
`action` string,
`contentType` string,
`displayMills` string,
`itemId` string,
`activityId` string,
`server_time` string)
PARTITIONED BY (dt string)
stored as parquet
location '/warehouse/gmall/dwd/dwd_ad_log/'
TBLPROPERTIES('parquet.compression'='lzo');

--装载
set hive.exec.dynamic.partition.mode=nonstrict;

insert overwrite table dwd_ad_log
PARTITION (dt='2020-03-15')
select 
mid_id,
user_id,
version_code,
version_name,
lang,
source,
os,
area,
model,
brand,
sdk_version,
gmail,
height_width,
app_time,
network,
lng,
lat,
get_json_object(event_json,'$.kv.entry') entry,
get_json_object(event_json,'$.kv.action') action,
get_json_object(event_json,'$.kv.contentType') contentType,
get_json_object(event_json,'$.kv.displayMills') displayMills,
get_json_object(event_json,'$.kv.itemId') itemId,
get_json_object(event_json,'$.kv.activityId') activityId,
server_time
from dwd_base_event_log 
where dt='2020-03-15' and event_name='ad';
===========================
5.dwd_notification_log
===========================
drop table if exists dwd_notification_log;
CREATE EXTERNAL TABLE dwd_notification_log(
`mid_id` string,
`user_id` string, 
`version_code` string, 
`version_name` string, 
`lang` string,
`source` string, 
`os` string, 
`area` string, 
`model` string,
`brand` string, 
`sdk_version` string, 
`gmail` string, 
`height_width` string,  
`app_time` string,
`network` string, 
`lng` string, 
`lat` string, 
`action` string,
`noti_type` string,
`ap_time` string,
`content` string,
`server_time` string
)
PARTITIONED BY (dt string)
stored as parquet
location '/warehouse/gmall/dwd/dwd_notification_log/'
TBLPROPERTIES('parquet.compression'='lzo');

--装载
set hive.exec.dynamic.partition.mode=nonstrict;

insert overwrite table dwd_notification_log
PARTITION (dt='2020-03-15')
select 
mid_id,
user_id,
version_code,
version_name,
lang,
source,
os,
area,
model,
brand,
sdk_version,
gmail,
height_width,
app_time,
network,
lng,
lat,
get_json_object(event_json,'$.kv.action') action,
get_json_object(event_json,'$.kv.noti_type') noti_type,
get_json_object(event_json,'$.kv.ap_time') ap_time,
get_json_object(event_json,'$.kv.content') content,
server_time
from dwd_base_event_log
where dt='2020-03-15' and event_name='notification';
===========================
6.dwd_active_background_log
===========================
drop table if exists dwd_active_background_log;
CREATE EXTERNAL TABLE dwd_active_background_log(
`mid_id` string,
`user_id` string,
`version_code` string,
`version_name` string,
`lang` string,
`source` string,
`os` string,
`area` string,
`model` string,
`brand` string,
`sdk_version` string,
`gmail` string,
 `height_width` string,
`app_time` string,
`network` string,
`lng` string,
`lat` string,
`active_source` string,
`server_time` string
)
PARTITIONED BY (dt string)
stored as parquet
location '/warehouse/gmall/dwd/dwd_background_log/'
TBLPROPERTIES('parquet.compression'='lzo');

--装载数据
set hive.exec.dynamic.partition.mode=nonstrict;

insert overwrite table dwd_active_background_log
PARTITION (dt='2020-03-15')
select 
mid_id,
user_id,
version_code,
version_name,
lang,
source,
os,
area,
model,
brand,
sdk_version,
gmail,
height_width,
app_time,
network,
lng,
lat,
get_json_object(event_json,'$.kv.active_source') active_source,
server_time
from dwd_base_event_log
where dt='2020-03-15' and event_name='active_background';
===========================
7.dwd_comment_log
===========================
drop table if exists dwd_comment_log;
CREATE EXTERNAL TABLE dwd_comment_log(
`mid_id` string,
`user_id` string,
`version_code` string,
`version_name` string,
`lang` string,
`source` string,
`os` string,
`area` string,
`model` string,
`brand` string,
`sdk_version` string,
`gmail` string,
`height_width` string,
`app_time` string,
`network` string,
`lng` string,
`lat` string,
`comment_id` int,
`userid` int,
`p_comment_id` int, 
`content` string,
`addtime` string,
`other_id` int,
`praise_count` int,
`reply_count` int,
`server_time` string
)
PARTITIONED BY (dt string)
stored as parquet
location '/warehouse/gmall/dwd/dwd_comment_log/'
TBLPROPERTIES('parquet.compression'='lzo');

--装载数据
set hive.exec.dynamic.partition.mode=nonstrict;

insert overwrite table dwd_comment_log
PARTITION (dt='2020-03-15')
select 
mid_id,
user_id,
version_code,
version_name,
lang,
source,
os,
area,
model,
brand,
sdk_version,
gmail,
height_width,
app_time,
network,
lng,
lat,
get_json_object(event_json,'$.kv.comment_id') comment_id,
get_json_object(event_json,'$.kv.userid') userid,
get_json_object(event_json,'$.kv.p_comment_id') p_comment_id,
get_json_object(event_json,'$.kv.content') content,
get_json_object(event_json,'$.kv.addtime') addtime,
get_json_object(event_json,'$.kv.other_id') other_id,
get_json_object(event_json,'$.kv.praise_count') praise_count,
get_json_object(event_json,'$.kv.reply_count') reply_count,
server_time
from dwd_base_event_log
where dt='2020-03-15' and event_name='comment';
===========================
8.dwd_favorites_log
===========================
drop table if exists dwd_favorites_log;
CREATE EXTERNAL TABLE dwd_favorites_log(
`mid_id` string,
`user_id` string, 
`version_code` string, 
`version_name` string, 
`lang` string, 
`source` string, 
`os` string, 
`area` string, 
`model` string,
`brand` string, 
`sdk_version` string, 
`gmail` string, 
`height_width` string,  
`app_time` string,
`network` string, 
`lng` string, 
`lat` string, 
`id` int, 
`course_id` int, 
`userid` int,
`add_time` string,
`server_time` string
)
PARTITIONED BY (dt string)
stored as parquet
location '/warehouse/gmall/dwd/dwd_favorites_log/'
TBLPROPERTIES('parquet.compression'='lzo');

--装载数据
set hive.exec.dynamic.partition.mode=nonstrict;

insert overwrite table dwd_favorites_log
PARTITION (dt='2020-03-15')
select 
mid_id,
user_id,
version_code,
version_name,
lang,
source,
os,
area,
model,
brand,
sdk_version,
gmail,
height_width,
app_time,
network,
lng,
lat,
get_json_object(event_json,'$.kv.id') id,
get_json_object(event_json,'$.kv.course_id') course_id,
get_json_object(event_json,'$.kv.userid') userid,
get_json_object(event_json,'$.kv.add_time') add_time,
server_time
from dwd_base_event_log 
where dt='2020-03-15' and event_name='favorites';
===========================
9.dwd_praise_log
===========================
drop table if exists dwd_praise_log;
CREATE EXTERNAL TABLE dwd_praise_log(
`mid_id` string,
`user_id` string, 
`version_code` string, 
`version_name` string, 
`lang` string, 
`source` string, 
`os` string, 
`area` string, 
`model` string,
`brand` string, 
`sdk_version` string, 
`gmail` string, 
`height_width` string,  
`app_time` string,
`network` string, 
`lng` string, 
`lat` string, 
`id` string, 
`userid` string, 
`target_id` string,
`type` string,
`add_time` string,
`server_time` string
)
PARTITIONED BY (dt string)
stored as parquet
location '/warehouse/gmall/dwd/dwd_praise_log/'
TBLPROPERTIES('parquet.compression'='lzo');

--装载数据
set hive.exec.dynamic.partition.mode=nonstrict;

insert overwrite table dwd_praise_log
PARTITION (dt='2020-03-15')
select 
mid_id,
user_id,
version_code,
version_name,
lang,
source,
os,
area,
model,
brand,
sdk_version,
gmail,
height_width,
app_time,
network,
lng,
lat,
get_json_object(event_json,'$.kv.id') id,
get_json_object(event_json,'$.kv.userid') userid,
get_json_object(event_json,'$.kv.target_id') target_id,
get_json_object(event_json,'$.kv.type') type,
get_json_object(event_json,'$.kv.add_time') add_time,
server_time
from dwd_base_event_log
where dt='2020-03-15' and event_name='praise';
===========================
10.dwd_error_log
===========================
drop table if exists dwd_error_log;
CREATE EXTERNAL TABLE dwd_error_log(
`mid_id` string,
`user_id` string, 
`version_code` string, 
`version_name` string, 
`lang` string, 
`source` string, 
`os` string, 
`area` string, 
`model` string,
`brand` string, 
`sdk_version` string, 
`gmail` string, 
`height_width` string,  
`app_time` string,
`network` string, 
`lng` string, 
`lat` string, 
`errorBrief` string, 
`errorDetail` string, 
`server_time` string)
PARTITIONED BY (dt string)
stored as parquet
location '/warehouse/gmall/dwd/dwd_error_log/'
TBLPROPERTIES('parquet.compression'='lzo');

--装载
set hive.exec.dynamic.partition.mode=nonstrict;

insert overwrite table dwd_error_log
PARTITION (dt='2020-03-15')
select 
mid_id,
user_id,
version_code,
version_name,
lang,
source,
os,
area,
model,
brand,
sdk_version,
gmail,
height_width,
app_time,
network,
lng,
lat,
get_json_object(event_json,'$.kv.errorBrief') errorBrief,
get_json_object(event_json,'$.kv.errorDetail') errorDetail,
server_time
from dwd_base_event_log 
where dt='2020-03-15' and event_name='error';
===========================
写个装载脚本好不啦
===========================
#!/bin/bash

# 定义变量方便修改
APP=gmall
hive=/opt/module/hive/bin/hive

# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n "$1" ] ;then
	do_date=$1
else 
	do_date=`date -d "-1 day" +%F`  
fi 

sql="
set hive.exec.dynamic.partition.mode=nonstrict;

insert overwrite table "$APP".dwd_display_log
PARTITION (dt='$do_date')
select 
	mid_id,
	user_id,
	version_code,
	version_name,
	lang,
	source,
	os,
	area,
	model,
	brand,
	sdk_version,
	gmail,
	height_width,
	app_time,
	network,
	lng,
	lat,
	get_json_object(event_json,'$.kv.action') action,
	get_json_object(event_json,'$.kv.goodsid') goodsid,
	get_json_object(event_json,'$.kv.place') place,
	get_json_object(event_json,'$.kv.extend1') extend1,
	get_json_object(event_json,'$.kv.category') category,
	server_time
from "$APP".dwd_base_event_log 
where dt='$do_date' and event_name='display';


insert overwrite table "$APP".dwd_newsdetail_log
PARTITION (dt='$do_date')
select 
	mid_id,
	user_id,
	version_code,
	version_name,
	lang,
	source,
	os,
	area,
	model,
	brand,
	sdk_version,
	gmail,
	height_width,
	app_time,
	network,
	lng,
	lat,
	get_json_object(event_json,'$.kv.entry') entry,
	get_json_object(event_json,'$.kv.action') action,
	get_json_object(event_json,'$.kv.goodsid') goodsid,
	get_json_object(event_json,'$.kv.showtype') showtype,
	get_json_object(event_json,'$.kv.news_staytime') news_staytime,
	get_json_object(event_json,'$.kv.loading_time') loading_time,
	get_json_object(event_json,'$.kv.type1') type1,
	get_json_object(event_json,'$.kv.category') category,
	server_time
from "$APP".dwd_base_event_log 
where dt='$do_date' and event_name='newsdetail';


insert overwrite table "$APP".dwd_loading_log
PARTITION (dt='$do_date')
select 
	mid_id,
	user_id,
	version_code,
	version_name,
	lang,
	source,
	os,
	area,
	model,
	brand,
	sdk_version,
	gmail,
	height_width,
	app_time,
	network,
	lng,
	lat,
	get_json_object(event_json,'$.kv.action') action,
	get_json_object(event_json,'$.kv.loading_time') loading_time,
	get_json_object(event_json,'$.kv.loading_way') loading_way,
	get_json_object(event_json,'$.kv.extend1') extend1,
	get_json_object(event_json,'$.kv.extend2') extend2,
	get_json_object(event_json,'$.kv.type') type,
	get_json_object(event_json,'$.kv.type1') type1,
	server_time
from "$APP".dwd_base_event_log 
where dt='$do_date' and event_name='loading';


insert overwrite table "$APP".dwd_ad_log
PARTITION (dt='$do_date')
select 
	mid_id,
	user_id,
	version_code,
	version_name,
	lang,
	source,
	os,
	area,
	model,
	brand,
	sdk_version,
	gmail,
	height_width,
	app_time,
	network,
	lng,
	lat,
    get_json_object(event_json,'$.kv.entry') entry,
    get_json_object(event_json,'$.kv.action') action,
    get_json_object(event_json,'$.kv.contentType') contentType,
    get_json_object(event_json,'$.kv.displayMills') displayMills,
    get_json_object(event_json,'$.kv.itemId') itemId,
    get_json_object(event_json,'$.kv.activityId') activityId,
	server_time
from "$APP".dwd_base_event_log 
where dt='$do_date' and event_name='ad';


insert overwrite table "$APP".dwd_notification_log
PARTITION (dt='$do_date')
select 
	mid_id,
	user_id,
	version_code,
	version_name,
	lang,
	source,
	os,
	area,
	model,
	brand,
	sdk_version,
	gmail,
	height_width,
	app_time,
	network,
	lng,
	lat,
	get_json_object(event_json,'$.kv.action') action,
	get_json_object(event_json,'$.kv.noti_type') noti_type,
	get_json_object(event_json,'$.kv.ap_time') ap_time,
	get_json_object(event_json,'$.kv.content') content,
	server_time
from "$APP".dwd_base_event_log 
where dt='$do_date' and event_name='notification';


insert overwrite table "$APP".dwd_active_background_log
PARTITION (dt='$do_date')
select 
	mid_id,
	user_id,
	version_code,
	version_name,
	lang,
	source,
	os,
	area,
	model,
	brand,
	sdk_version,
	gmail,
	height_width,
	app_time,
	network,
	lng,
	lat,
	get_json_object(event_json,'$.kv.active_source') active_source,
	server_time
from "$APP".dwd_base_event_log 
where dt='$do_date' and event_name='active_background';


insert overwrite table "$APP".dwd_comment_log
PARTITION (dt='$do_date')
select 
	mid_id,
	user_id,
	version_code,
	version_name,
	lang,
	source,
	os,
	area,
	model,
	brand,
	sdk_version,
	gmail,
	height_width,
	app_time,
	network,
	lng,
	lat,
	get_json_object(event_json,'$.kv.comment_id') comment_id,
	get_json_object(event_json,'$.kv.userid') userid,
	get_json_object(event_json,'$.kv.p_comment_id') p_comment_id,
	get_json_object(event_json,'$.kv.content') content,
	get_json_object(event_json,'$.kv.addtime') addtime,
	get_json_object(event_json,'$.kv.other_id') other_id,
	get_json_object(event_json,'$.kv.praise_count') praise_count,
	get_json_object(event_json,'$.kv.reply_count') reply_count,
	server_time
from "$APP".dwd_base_event_log 
where dt='$do_date' and event_name='comment';


insert overwrite table "$APP".dwd_favorites_log
PARTITION (dt='$do_date')
select 
	mid_id,
	user_id,
	version_code,
	version_name,
	lang,
	source,
	os,
	area,
	model,
	brand,
	sdk_version,
	gmail,
	height_width,
	app_time,
	network,
	lng,
	lat,
	get_json_object(event_json,'$.kv.id') id,
	get_json_object(event_json,'$.kv.course_id') course_id,
	get_json_object(event_json,'$.kv.userid') userid,
	get_json_object(event_json,'$.kv.add_time') add_time,
	server_time
from "$APP".dwd_base_event_log 
where dt='$do_date' and event_name='favorites';


insert overwrite table "$APP".dwd_praise_log
PARTITION (dt='$do_date')
select 
	mid_id,
	user_id,
	version_code,
	version_name,
	lang,
	source,
	os,
	area,
	model,
	brand,
	sdk_version,
	gmail,
	height_width,
	app_time,
	network,
	lng,
	lat,
	get_json_object(event_json,'$.kv.id') id,
	get_json_object(event_json,'$.kv.userid') userid,
	get_json_object(event_json,'$.kv.target_id') target_id,
	get_json_object(event_json,'$.kv.type') type,
	get_json_object(event_json,'$.kv.add_time') add_time,
	server_time
from "$APP".dwd_base_event_log 
where dt='$do_date' and event_name='praise';


insert overwrite table "$APP".dwd_error_log
PARTITION (dt='$do_date')
select 
	mid_id,
	user_id,
	version_code,
	version_name,
	lang,
	source,
	os,
	area,
	model,
	brand,
	sdk_version,
	gmail,
	height_width,
	app_time,
	network,
	lng,
	lat,
	get_json_object(event_json,'$.kv.errorBrief') errorBrief,
	get_json_object(event_json,'$.kv.errorDetail') errorDetail,
	server_time
from "$APP".dwd_base_event_log 
where dt='$do_date' and event_name='error';
"

$hive -e "$sql"


===========================
  DWD层业务数据维度建模
  建表语句建表时选取的是维度，和度量值 以及从ods层所需要的字段
  事务性事实表对照增量 ，周期性对照全量
  周期型快照事实表对应全量，那两个特殊的全量
  累积型快照事实表对应新增及其变化
===========================
订单明细事实表（事务型事实表） dwd_fact_order_detail
===========================
===========================
drop table if exists dwd_fact_order_detail;
create external table dwd_fact_order_detail (
    `id` string COMMENT '',
    `order_id` string COMMENT '',
    `province_id` string COMMENT '',
    `user_id` string COMMENT '',
    `sku_id` string COMMENT '',
    `create_time` string COMMENT '',
    `total_amount` decimal(20,2) COMMENT '',
    `sku_num` bigint COMMENT ''
) 
PARTITIONED BY (`dt` string)
stored as parquet
location '/warehouse/gmall/dwd/dwd_fact_order_detail/'
tblproperties ("parquet.compression"="lzo");
===========================
数据装载
===========================
从ods_order_detail(增量)中获取大多数据，从ods_order_info（只是获取province）
insert OVER）RITE table dwd_fact_order_detail partition(dt = '2020-03-15') 
select
    od.id,
    od.order_id,
    oi.province_id,
    od.user_id,
    od.sku_id,
    od.create_time,
    od.order_price*od.sku_num,
    od.sku_num
from 
(
    select * from ods_order_detail where dt='2020-03-15'
) od
join 
(
    select * from ods_order_info where dt='2020-03-15'
) oi
on od.order_id=oi.id;
===========================
支付事实表（事务型事实表） dwd_fact_payment_info
===========================
建表语句
===========================
drop table if exists dwd_fact_payment_info;
create external table dwd_fact_payment_info (
    `id` string COMMENT '',
    `order_id` string COMMENT '',
    `province_id` string COMMENT '',
    `user_id` string COMMENT '',
    `out_trade_no` string COMMENT '',
    `payment_type` string COMMENT '',
    `payment_time` string COMMENT '',
    `payment_amount` decimal(20,2) COMMENT ''
) 
PARTITIONED BY (`dt` string)
stored as parquet
location '/warehouse/gmall/dwd/dwd_fact_payment_info/'
tblproperties ("parquet.compression"="lzo");
===========================
数据装载
来自于ods_payment_info(增量)和ods_order_info（只是获取province）获取数据
===========================
insert OVERWRITE table dwd_fact_payment_info partition(dt = '2020-03-15')
select
    pi.id,
    pi.order_id,
    oi.province_id,
    pi.user_id,
    pi.out_trade_no,
    pi.payment_type,
    pi.payment_time,
    pi.total_amount
from
(
    select * from ods_payment_info where dt='2020-03-15'
)pi
join
(
    select id,province_id from ods_order_info where dt='2020-03-15'
)oi
on pi.order_id=oi.id;
===========================
退款事实表（事务型事实表） dwd_fact_order_refund_info
===========================
建表语句
===========================
drop table if exists dwd_fact_order_refund_info;
create external table dwd_fact_order_refund_info(
    `id` string COMMENT '编号',
    `user_id` string COMMENT '用户ID',
    `order_id` string COMMENT '订单ID',
    `sku_id` string COMMENT '商品ID',
    `refund_type` string COMMENT '退款类型',
    `refund_num` string COMMENT '退款件数',
    `refund_amount` string COMMENT '退款金额',
    `refund_reason_type` string COMMENT '退款原因类型',
    `create_time` string COMMENT '退款时间'
) COMMENT '退款事实表'
PARTITIONED BY (`dt` string)
row format delimited fields terminated by '\t'
location '/warehouse/gmall/dwd/dwd_fact_order_refund_info/';
===========================
数据装载来自ods_order_refund(增量)
===========================
insert overwrite table dwd_fact_order_refund_info partition(dt='2020-03-15')
select
    id,
    user_id,
    order_id,
    sku_id,
    refund_type,
    refund_num,
    refund_amount,
    refund_reason_type,
    create_time
from ods_order_refund_info
where dt='2020-03-15';
===========================
评价事实表（事务型事实表） dwd_fact_comment_info
===========================
建表语句 
===========================
drop table if exists dwd_fact_comment_info;
create external table dwd_fact_comment_info(
    `id` string COMMENT '编号',
    `user_id` string COMMENT '用户ID',
    `sku_id` string COMMENT '商品sku',
    `spu_id` string COMMENT '商品spu',
    `order_id` string COMMENT '订单ID',
    `appraise` string COMMENT '评价',
    `create_time` string COMMENT '评价时间'
) COMMENT '退款事实表'
PARTITIONED BY (`dt` string)
row format delimited fields terminated by '\t'
location '/warehouse/gmall/dwd/dwd_fact_comment_info/';
===========================
数据装载来自ods_comment_info（增量）
===========================
insert overwrite table dwd_fact_comment_info partition(dt='2020-03-15')
select
    id,
    user_id,
    sku_id,
    spu_id,
    order_id,
    appraise,
    create_time
from ods_comment_info
where dt='2020-03-15';


===========================
周期性快照事实表里面都是最新的数据
收藏事实表（周期性快照事实表） dwd_fact_favor_info
===========================
建表语句
===========================
drop table if exists dwd_fact_favor_info;
create external table dwd_fact_favor_info(
    `id` string COMMENT '编号',
    `user_id` string  COMMENT '用户id',
    `sku_id` string  COMMENT 'skuid',
    `spu_id` string  COMMENT 'spuid',
    `is_cancel` string  COMMENT '是否取消',
    `create_time` string  COMMENT '收藏时间',
    `cancel_time` string  COMMENT '取消时间'
) COMMENT '收藏事实表'
PARTITIONED BY (`dt` string)
row format delimited fields terminated by '\t'
location '/warehouse/gmall/dwd/dwd_fact_favor_info/';
===========================
数据装载来自ods_favor_info（全量----------cart_info新增及变化（特殊这里使用的全量））
===========================
insert overwrite table dwd_fact_favor_info partition(dt='2020-03-15')
select
    id,
    user_id,
    sku_id,
    spu_id,
    is_cancel,
    create_time,
    cancel_time
from ods_favor_info
where dt='2020-03-15';
===========================
加购物车事实表（周期性快照事实表） dwd_fact_cart_info
===========================
建表语句
===========================
drop table if exists dwd_fact_cart_info;
create external table dwd_fact_cart_info(
    `id` string COMMENT '编号',
    `user_id` string  COMMENT '用户id',
    `sku_id` string  COMMENT 'skuid',
    `cart_price` string  COMMENT '放入购物车时价格',
    `sku_num` string  COMMENT '数量',
    `sku_name` string  COMMENT 'sku名称 (冗余)',
    `create_time` string  COMMENT '创建时间',
    `operate_time` string COMMENT '修改时间',
    `is_ordered` string COMMENT '是否已经下单',
    `order_time` string  COMMENT '下单时间'
) COMMENT '加购事实表'
PARTITIONED BY (`dt` string)
row format delimited fields terminated by '\t'
location '/warehouse/gmall/dwd/dwd_fact_cart_info/';
===========================
数据装载来自于ods_cart_info（新增及变（特殊这里使用的全量）） ---取数据时候取是当前得所有数据，是一个全量表
===========================
insert overwrite table dwd_fact_cart_info partition(dt='2020-03-15')
select
    id,
    user_id,
    sku_id,
    cart_price,
    sku_num,
    sku_name,
    create_time,
    operate_time,
    is_ordered,
    order_time
from ods_cart_info
where dt='2020-03-15';

===========================
优惠券领用详情（累积型事实表） dwd_fact_coupon_use
===========================
建表语句
===========================
drop table if exists dwd_fact_coupon_use;
create external table dwd_fact_coupon_use(
    `id` string COMMENT '编号',
    `coupon_id` string  COMMENT '优惠券ID',
    `user_id` string  COMMENT 'skuid',
    `order_id` string  COMMENT 'spuid',
    `coupon_status` string  COMMENT '优惠券状态',
    `get_time` string  COMMENT '领取时间',
    `using_time` string  COMMENT '使用时间(下单)',
    `used_time` string  COMMENT '使用时间(支付)'
) COMMENT '优惠券领用事实表'
PARTITIONED BY (`dt` string)
row format delimited fields terminated by '\t'
location '/warehouse/gmall/dwd/dwd_fact_coupon_use/';
===========================
数据装载来自于ods_coupon_use（新增及变化）以及dwd_fact_coupon_use
===========================
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd_fact_coupon_use partition(dt)
select
    if(new.id is null,old.id,new.id),
    if(new.coupon_id is null,old.coupon_id,new.coupon_id),
    if(new.user_id is null,old.user_id,new.user_id),
    if(new.order_id is null,old.order_id,new.order_id),
    if(new.coupon_status is null,old.coupon_status,new.coupon_status),
    if(new.get_time is null,old.get_time,new.get_time),
    if(new.using_time is null,old.using_time,new.using_time),
    if(new.used_time is null,old.used_time,new.used_time),
    date_format(if(new.get_time is null,old.get_time,new.get_time),'yyyy-MM-dd')
from
(   --old里面是需要修改的数据，可以从事实表中得到需要修改数据的分区
    select
        id,
        coupon_id,
        user_id,
        order_id,
        coupon_status,
        get_time,
        using_time,
        used_time
    from dwd_fact_coupon_use
    where dt in --选取事实表中需要修改的数据，可以要修改得到数据的分区
    (
        --返回当天数据需要修改的时间
        select
            date_format(get_time,'yyyy-MM-dd')
        from ods_coupon_use  --这里面增添的数据都是新增及变化的，返回的是格式化的时间
        where dt='2020-03-15'
    )
)old
full outer join --全外连接比较为空的数据，让为空的数据不能返回~~
(   --这里得到是新增及变化的数据
    select
        id,
        coupon_id,
        user_id,
        order_id,
        coupon_status,
        get_time,
        using_time,
        used_time
    from ods_coupon_use
    where dt='2020-03-15'
)new
on old.id=new.id;
===========================
订单事实表（累积型快照事实表） dwd_fact_order_info
===========================
建表语句
===========================
drop table if exists dwd_fact_order_info;
create external table dwd_fact_order_info (
    `id` string COMMENT '订单编号',
    `order_status` string COMMENT '订单状态',
    `user_id` string COMMENT '用户id',
    `out_trade_no` string COMMENT '支付流水号',
    `create_time` string COMMENT '创建时间(未支付状态)',
    `payment_time` string COMMENT '支付时间(已支付状态)',
    `cancel_time` string COMMENT '取消时间(已取消状态)',
    `finish_time` string COMMENT '完成时间(已完成状态)',
    `refund_time` string COMMENT '退款时间(退款中状态)',
    `refund_finish_time` string COMMENT '退款完成时间(退款完成状态)',
    `province_id` string COMMENT '省份ID',
    `activity_id` string COMMENT '活动ID',
    `original_total_amount` string COMMENT '原价金额',
    `benefit_reduce_amount` string COMMENT '优惠金额',
    `feight_fee` string COMMENT '运费',
    `final_total_amount` decimal(10,2) COMMENT '订单金额'
) 
PARTITIONED BY (`dt` string)
stored as parquet
location '/warehouse/gmall/dwd/dwd_fact_order_info/'
tblproperties ("parquet.compression"="lzo");
===========================
数据装载来自于老的dwd_fact_order_info，以及ods_order_info（新增及变化）的数据，同时具有ods_status_log的订单状态日志变换成Map
===========================
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd_fact_order_info partition(dt)
select
    if(new.id is null,old.id,new.id),
    if(new.order_status is null,old.order_status,new.order_status),
    if(new.user_id is null,old.user_id,new.user_id),
    if(new.out_trade_no is null,old.out_trade_no,new.out_trade_no),
    --拆开map集合，通过key值获取状态修改的时间
    if(new.tms['1001'] is null,old.create_time,new.tms['1001']),--1001对应未支付状态
    if(new.tms['1002'] is null,old.payment_time,new.tms['1002']),
    if(new.tms['1003'] is null,old.cancel_time,new.tms['1003']),
    if(new.tms['1004'] is null,old.finish_time,new.tms['1004']),
    if(new.tms['1005'] is null,old.refund_time,new.tms['1005']),
    if(new.tms['1006'] is null,old.refund_finish_time,new.tms['1006']),
    if(new.province_id is null,old.province_id,new.province_id),
    if(new.activity_id is null,old.activity_id,new.activity_id),
    if(new.original_total_amount is null,old.original_total_amount,new.original_total_amount),
    if(new.benefit_reduce_amount is null,old.benefit_reduce_amount,new.benefit_reduce_amount),
    if(new.feight_fee is null,old.feight_fee,new.feight_fee),
    if(new.final_total_amount is null,old.final_total_amount,new.final_total_amount),
    date_format(if(new.tms['1001'] is null,old.create_time,new.tms['1001']),'yyyy-MM-dd')
from
(
    select
        id,
        order_status,
        user_id,
        out_trade_no,
        create_time,
        payment_time,
        cancel_time,
        finish_time,
        refund_time,
        refund_finish_time,
        province_id,
        activity_id,
        original_total_amount,
        benefit_reduce_amount,
        feight_fee,
        final_total_amount
    from dwd_fact_order_info
    where dt
    in
    (
    select
      date_format(operate_time,'%Y-%m-%d')
    from ods_order_info
    where dt='2020-03-15'
    )
)old
full outer join
(   --新修改的数据和增加的订单
    select
        info.id,
        info.order_status,
        info.user_id,
        info.out_trade_no,
        info.province_id,
        act.activity_id,
        log.tms,
        info.original_total_amount,
        info.benefit_reduce_amount,
        info.feight_fee,
        info.final_total_amount
    from
    (   --返回订单和map状态集合
        select
            order_id,
            str_to_map(concat_ws(',',collect_set(concat(order_status,'=',operate_time))),',','=') tms 
            --拼接相同订单的各种状态和状态的时间，
            --让后转换为集合，然后转换用concat_ws拼接成 ，相分隔
            --然后转换成map
        from ods_order_status_log
        where dt='2020-03-15'
        group by order_id
    )log
    join
    (   --
        select * from ods_order_info where dt='2020-03-15'
    )info
    on log.order_id=info.id
    left join
    (
        select * from ods_activity_order where dt='2020-03-15'
    )act
    on log.order_id=act.order_id
)new
on old.id=new.id;








===========================
维度表
===========================
商品维度表（全量表） dwd_dim_sku_info
===========================
建表语句
===========================
DROP TABLE IF EXISTS `dwd_dim_sku_info`;
CREATE EXTERNAL TABLE `dwd_dim_sku_info` (
    `id` string COMMENT '商品id',
    `spu_id` string COMMENT 'spuid',
    `price` double COMMENT '商品价格',
    `sku_name` string COMMENT '商品名称',
    `sku_desc` string COMMENT '商品描述',
    `weight` double COMMENT '重量',
    `tm_id` string COMMENT '品牌id',
    `tm_name` string COMMENT '品牌名称',
    `category3_id` string COMMENT '三级分类id',
    `category2_id` string COMMENT '二级分类id',
    `category1_id` string COMMENT '一级分类id',
    `category3_name` string COMMENT '三级分类名称',
    `category2_name` string COMMENT '二级分类名称',
    `category1_name` string COMMENT '一级分类名称',
    `create_time` string COMMENT '创建时间'
) 
COMMENT '商品维度表'
PARTITIONED BY (`dt` string)
stored as parquet
location '/warehouse/gmall/dwd/dwd_dim_sku_info/'
tblproperties ("parquet.compression"="lzo");
===========================
数据装载来自于ods_sku_info（全量）,ods_base_category123（全量）,ods_base_trademark（全量）
===========================
insert overwrite table dwd_dim_sku_info partition(dt='2020-03-15')
select  
    sku.id,
    sku.spu_id,
    sku.price,
    sku.sku_name,
    sku.sku_desc,
    sku.weight,
    sku.tm_id,
    ob.tm_name,
    sku.category3_id,
    c2.id category2_id,
    c1.id category1_id,
    c3.name category3_name,
    c2.name category2_name,
    c1.name category1_name,
    sku.create_time
from
(
    select * from ods_sku_info where dt='2020-03-15'
)sku
join 
(
    select * from ods_base_category3 where dt='2020-03-15'
)c3 on sku.category3_id=c3.id 
join 
(
    select * from ods_base_category2 where dt='2020-03-15'
)c2 on c3.category2_id=c2.id 
join 
(
    select * from ods_base_category1 where dt='2020-03-15'
)c1 on c2.category1_id=c1.id 
join
(
    select * from ods_base_trademark where dt='2020-03-15'
)ob on sku.tm_id=ob.tm_id;
===========================
优惠劵维度表（全量） dwd_dim_coupon_info
===========================
建表语句
===========================
drop table if exists dwd_dim_coupon_info;
create external table dwd_dim_coupon_info(
    `id` string COMMENT '购物券编号',
    `coupon_name` string COMMENT '购物券名称',
    `coupon_type` string COMMENT '购物券类型 1 现金券 2 折扣券 3 满减券 4 满件打折券',
    `condition_amount` string COMMENT '满额数',
    `condition_num` string COMMENT '满件数',
    `activity_id` string COMMENT '活动编号',
    `benefit_amount` string COMMENT '减金额',
    `benefit_discount` string COMMENT '折扣',
    `create_time` string COMMENT '创建时间',
    `range_type` string COMMENT '范围类型 1、商品 2、品类 3、品牌',
    `spu_id` string COMMENT '商品id',
    `tm_id` string COMMENT '品牌id',
    `category3_id` string COMMENT '品类id',
    `limit_num` string COMMENT '最多领用次数',
    `operate_time`  string COMMENT '修改时间',
    `expire_time`  string COMMENT '过期时间'
) COMMENT '优惠券信息表'
PARTITIONED BY (`dt` string)
row format delimited fields terminated by '\t'
location '/warehouse/gmall/dwd/dwd_dim_coupon_info/';
===========================
数据装载来自于ods_coupon_info（全量）  
===========================
insert overwrite table dwd_dim_coupon_info partition(dt='2020-03-15')
select
    id,
    coupon_name,
    coupon_type,
    condition_amount,
    condition_num,
    activity_id,
    benefit_amount,
    benefit_discount,
    create_time,
    range_type,
    spu_id,
    tm_id,
    category3_id,
    limit_num,
    operate_time,
    expire_time
from ods_coupon_info
where dt='2020-03-15';
===========================
营销活动信息表（全量维度事实表） dwd_dim_activity_info
===========================
建表语句
===========================
drop table if exists dwd_dim_activity_info;
create external table dwd_dim_activity_info(
    `id` string COMMENT '编号',
    `activity_name` string  COMMENT '活动名称',
    `activity_type` string  COMMENT '活动类型',
    `start_time` string  COMMENT '开始时间',
    `end_time` string  COMMENT '结束时间',
    `create_time` string  COMMENT '创建时间'
) COMMENT '营销活动信息表'
PARTITIONED BY (`dt` string)
row format delimited fields terminated by '\t'
location '/warehouse/gmall/dwd/dwd_dim_activity_info/';
===========================
数据装载来自于ods_activity_info
===========================
insert overwrite table dwd_dim_activity_info partition(dt='2020-03-15')
select
    id,
    activity_name,
    activity_type,
    start_time,
    end_time,
    create_time
from ods_activity_info
where dt='2020-03-15';
===========================
地区维度表（特殊） dwd_dim_base_province
===========================
建表语句
===========================
DROP TABLE IF EXISTS `dwd_dim_base_province`;
CREATE EXTERNAL TABLE `dwd_dim_base_province` (
    `id` string COMMENT 'id',
    `province_name` string COMMENT '省市名称',
    `area_code` string COMMENT '地区编码',
    `iso_code` string COMMENT 'ISO编码',
    `region_id` string COMMENT '地区id',
    `region_name` string COMMENT '地区名称'
) 
COMMENT '地区省市表'
location '/warehouse/gmall/dwd/dwd_dim_base_province/';
===========================
数据装载来自于ods_base_province和ods_base_region
===========================
insert overwrite table dwd_dim_base_province
select 
    bp.id,
    bp.name,
    bp.area_code,
    bp.iso_code,
    bp.region_id,
    br.region_name
from ods_base_province bp
join ods_base_region br
on bp.region_id=br.id;
===========================
时间维度表（特殊） dwd_dim_date_info
===========================
建表语句
===========================
DROP TABLE IF EXISTS `dwd_dim_date_info`;
CREATE EXTERNAL TABLE `dwd_dim_date_info`  (
    `date_id` string ,
    `week_id` int,
    `week_day` int,
    `day` int,
    `month` int,
    `quarter` int,
    `year` int,
    `is_workday` int,
    `holiday_id` int
)
row format delimited fields terminated by '\t'
location '/warehouse/gmall/dwd/dwd_dim_date_info/';
===========================
数据装载
===========================
load data local inpath '/opt/module/db_log/date_info.txt' into table dwd_dim_date_info;

===========================
用户维度表（拉链表） dwd_dim_user_info_his  --这里是全表导入，修改完成之后保留前面的数据以及新增
===========================
建表语句
===========================
drop table if exists dwd_dim_user_info_his;
create external table dwd_dim_user_info_his(
    `id` string COMMENT 'id',
    `name` string COMMENT '', 
    `birthday` string COMMENT '',
    `gender` string COMMENT '',
    `email` string COMMENT '',
`user_level` string COMMENT '',
`create_time` string COMMENT '',
`operate_time` string COMMENT '',
`start_date`  string COMMENT '有效开始日期',
`end_date`  string COMMENT '有效结束日期'
) COMMENT '订单拉链表'
stored as parquet
location '/warehouse/gmall/dwd/dwd_dim_user_info_his/'
tblproperties ("parquet.compression"="lzo");

初始化导入:生产情况需要使用sqoop单独做一次全量导入
本次导入：使用ods_user_info导入
insert overwrite table dwd_dim_user_info_his
select
    id,
    name,
    birthday,
    gender,
    email,
    user_level,
    create_time,
    operate_time,
    '2020-03-15',
    '9999-99-99'
from ods_user_info oi
where oi.dt='2020-03-15';

时间到了 2020-03-16
获取 2020-03-16 变动的数据 user_info（增量及变化）

将 2020-03-16 变动的数据 将 2020-03-15 的拉链表合并

--新数据

insert overwrite table dwd_dim_user_info_his_tmp
select * from 
(
  --返回原来的数据
select 
    id,
    name,
    birthday,
    gender,
    email,
    user_level,
    create_time,
    operate_time,
    '2020-03-15' start_date,
    '9999-99-99' end_date
from ods_user_info where dt='2020-03-15'

union all
--修改完成后的数据
select 
    uh.id,
    uh.name,
    uh.birthday,
    uh.gender,
    uh.email,
    uh.user_level,
    uh.create_time,
    uh.operate_time,
    uh.start_date,
    if(ui.id is not null  and uh.end_date='9999-99-99', date_add(ui.dt,-1), uh.end_date) end_date
from dwd_dim_user_info_his uh 
left join 
     (
      --当天全部用户数据
      select
      *
      from ods_user_info
      where dt='2020-03-15'
      ) ui
     on uh.id=ui.id
)his 
order by his.id, start_date;

===========================
数据装载来自于临时表以及dwd_dim_user_info_his以及ods_user_info
===========================
insert overwrite table dwd_dim_user_info_his 
select * from dwd_dim_user_info_his_tmp;















==============================================================================================================================================
--------------------------------------------------------DWS层---------------------------------------------------------------------------------
===============================================这一层考虑用户需求，数据通过DWD层获取，根据需求计算数据，以及着重获取当天的数据量
==============================================================================================================================================================
====================DWS表中是DWT所对应的当天行为数据，以及建表时的维度考虑是从DWD的维度表为依据，里面的数据是从DWD数据所得的事实表插入DWT表中获取====================================================

=====================================================DWS从事实表获取次数，从维度表获取信息===============================

----------------------------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------
====================================================
每日购买行为
====================================================
drop table if exists dws_sale_detail_daycount;
create external table dws_sale_detail_daycount
(   
    user_id   string  comment '用户 id',
    sku_id    string comment '商品 id',
    user_gender  string comment '用户性别',
    user_age string  comment '用户年龄',
    user_level string comment '用户等级',
    order_price decimal(10,2) comment '商品价格',
    sku_name string   comment '商品名称',
    sku_tm_id string   comment '品牌id',
    sku_category3_id string comment '商品三级品类id',
    sku_category2_id string comment '商品二级品类id',
    sku_category1_id string comment '商品一级品类id',
    sku_category3_name string comment '商品三级品类名称',
    sku_category2_name string comment '商品二级品类名称',
    sku_category1_name string comment '商品一级品类名称',
    spu_id  string comment '商品 spu',
    sku_num  int comment '购买个数',
    order_count bigint comment '当日下单单数',
    order_amount decimal(16,2) comment '当日下单金额'
) COMMENT '每日购买行为'
PARTITIONED BY (`dt` string)
stored as parquet
location '/warehouse/gmall/dws/dws_sale_detail_daycount/'
tblproperties ("parquet.compression"="lzo");
==========================================
数据装载dwd_fact_order_detail dwd_dim_user_info_his dwd_dim_sku_info
==========================================
--过滤出下单的数据
insert overwrite table dws_sale_detail_daycount partition(dt='2020-03-15')
select
    op.user_id,
    op.sku_id,
    ui.gender,
    months_between('2020-03-15', ui.birthday)/12  age, 
    ui.user_level,
    price,
    sku_name,
    tm_id,
    category3_id,
    category2_id,
    category1_id,
    category3_name,
    category2_name,
    category1_name,
    spu_id,
    op.sku_num,
    op.order_count,
    op.order_amount 
from
(
    select
        user_id,
        sku_id,
        sum(sku_num) sku_num,
        count(*) order_count,
        sum(total_amount) order_amount
    from dwd_fact_order_detail
    where dt='2020-03-15'
    --分两组是要精确到用户id以及商品id 这里统计的是商品 所以要对商品进行分组
    group by user_id, sku_id
)op
join
(
    select
        *
    from dwd_dim_user_info_his
    where end_date='9999-99-99'
)ui on op.user_id = ui.id
join
(
    select
        *
    from dwd_dim_sku_info
    where dt='2020-03-15'
)si on op.sku_id = si.id;


==========================================







===========================
每日设备行为（从前端埋点处获取数据，统计每日的流量，比如活跃度，等）
===========================
建表语句  pv某个页面一天当中被访问的次数。uv是访问某个页面的人数有多少人
===========================
drop table if exists dws_uv_detail_daycount;
create external table dws_uv_detail_daycount
(
    `mid_id` string COMMENT '设备唯一标识',
    `user_id` string COMMENT '用户标识',
    `version_code` string COMMENT '程序版本号', 
    `version_name` string COMMENT '程序版本名', 
    `lang` string COMMENT '系统语言', 
    `source` string COMMENT '渠道号', 
    `os` string COMMENT '安卓系统版本', 
    `area` string COMMENT '区域', 
    `model` string COMMENT '手机型号', 
    `brand` string COMMENT '手机品牌', 
    `sdk_version` string COMMENT 'sdkVersion', 
    `gmail` string COMMENT 'gmail', 
    `height_width` string COMMENT '屏幕宽高',
    `app_time` string COMMENT '客户端日志产生时的时间',
    `network` string COMMENT '网络模式',
    `lng` string COMMENT '经度',
    `lat` string COMMENT '纬度',
    `login_count` bigint COMMENT '活跃次数'
)
partitioned by(dt string) --分区统计每日活跃
stored as parquet
location '/warehouse/gmall/dws/dws_uv_detail_daycount';
===========================
数据装载来自于dwd_start_log(只要当天有启动就会在dwd_start_log就会有数据，不管多少个midid，和userid)
===========================
insert overwrite table dws_uv_detail_daycount partition(dt='2020-03-15')
select  
    mid_id,--一个mid_id的数据里面可能会有不同的数据，比如说不同的userid，不同的网络，不同的经纬度等...，所以都进行拼接
    concat_ws('|', collect_set(user_id)) user_id, --首先将分组好的数据拼接成一块，然后为了数据方便，不进行集合化，使用concat_ws将集合里面的数据拼接成字符串
    concat_ws('|', collect_set(version_name)) version_name,
    concat_ws('|', collect_set(version_code)) version_code,
    concat_ws('|', collect_set(lang))lang,
    concat_ws('|', collect_set(source)) source,
    concat_ws('|', collect_set(os)) os,
    concat_ws('|', collect_set(area)) area, 
    concat_ws('|', collect_set(model)) model,
    concat_ws('|', collect_set(brand)) brand,
    concat_ws('|', collect_set(sdk_version)) sdk_version,
    concat_ws('|', collect_set(gmail)) gmail,
    concat_ws('|', collect_set(height_width)) height_width, 
    concat_ws('|', collect_set(app_time)) app_time,
    concat_ws('|', collect_set(network)) network,
    concat_ws('|', collect_set(lng)) lng,
    concat_ws('|', collect_set(lat)) lat,
    count(*) login_count --count求出启动app的次数，每日某mid启动多次算一次
from dwd_start_log
where dt='2020-03-15'
group by mid_id;





===========================
设备主题宽表(全量表)（dwd_start_log为依据，数据从dws_uv_detail_daycount获取）dwt_uv_topic
===========================
建表语句
===========================
drop table if exists dwt_uv_topic;
create external table dwt_uv_topic
(
    `mid_id` string COMMENT '设备唯一标识',
    `user_id` string COMMENT '用户标识',
    `version_code` string COMMENT '程序版本号',
    `version_name` string COMMENT '程序版本名',
    `lang` string COMMENT '系统语言',
    `source` string COMMENT '渠道号',
    `os` string COMMENT '安卓系统版本',
    `area` string COMMENT '区域',
    `model` string COMMENT '手机型号',
    `brand` string COMMENT '手机品牌',
    `sdk_version` string COMMENT 'sdkVersion',
    `gmail` string COMMENT 'gmail',
    `height_width` string COMMENT '屏幕宽高',
    `app_time` string COMMENT '客户端日志产生时的时间',
    `network` string COMMENT '网络模式',
    `lng` string COMMENT '经度',
    `lat` string COMMENT '纬度',
    `login_date_first` string  comment '首次活跃时间',
    `login_date_last` string  comment '末次活跃时间',
    `login_day_count` bigint comment '当日活跃次数',
    `login_count` bigint comment '累积活跃天数'
)
stored as parquet
location '/warehouse/gmall/dwt/dwt_uv_detail_daycount';
===========================
数据装载来自于dwt_uv_topic获取老的数据 从dws_uv_detail_daycount获取新数据
===========================
insert overwrite table dwt_uv_topic --不用分区
select
    nvl(new.mid_id,old.mid_id),
    nvl(new.user_id,old.user_id),
    nvl(new.version_code,old.version_code),
    nvl(new.version_name,old.version_name),
    nvl(new.lang,old.lang),
    nvl(new.source,old.source),
    nvl(new.os,old.os),
    nvl(new.area,old.area),
    nvl(new.model,old.model),
    nvl(new.brand,old.brand),
    nvl(new.sdk_version,old.sdk_version),
    nvl(new.gmail,old.gmail),
    nvl(new.height_width,old.height_width),
    nvl(new.app_time,old.app_time),
    nvl(new.network,old.network),
    nvl(new.lng,old.lang),
    nvl(new.lat,old.lat),
    nvl(old.login_date_first,'2020-03-15'),
    if(new.login_count>0,'2020-03-15',old.login_date_last),
    nvl(new.login_count,0),
    --如果老的存在，则加上新的0 如果老的需要修改，则加上1，否则只有新的就+1天 --注意这里是天数
    nvl(old.login_count,0)+if(new.login_count>0,1,0)
from
(
    select
    --从本主题表中获取老的数据
        *
    from dwt_uv_topic
)old
full outer join
(
    select
    --从dws层日活跃层获取本日活跃的数据
        *
    from dws_uv_detail_daycount 
    where dt='2020-03-15'
)new
on old.mid_id=new.mid_id;

===========================
每日会员行为（从dwd事实表获取数据） dws_user_action_daycount
===========================
建表语句
===========================
drop table if exists dws_user_action_daycount;
create external table dws_user_action_daycount
(   
    user_id string comment '用户 id',
    login_count bigint comment '登录次数',
    cart_count bigint comment '加入购物车次数',
    cart_amount double comment '加入购物车金额',
    order_count bigint comment '下单次数',
    order_amount    decimal(16,2)  comment '下单金额',
    payment_count   bigint      comment '支付次数',
    payment_amount  decimal(16,2) comment '支付金额'
) COMMENT '每日用户行为'
PARTITIONED BY (`dt` string)
stored as parquet
location '/warehouse/gmall/dws/dws_user_action_daycount/'
tblproperties ("parquet.compression"="lzo");
===========================
数据装载来自于DWD层的dwd_start_log，dwd_fact_cart_info，dwd_fact_order_info，dwd_fact_payment_info
===========================
with --用法，简化子查询
tmp_login as
(
    select
        user_id,
        count(*) login_count
    from dwd_start_log
    where dt='2020-03-15'
    --小心user_id为空，因为会有没登陆的人登陆
    and user_id is not null
    group by user_id
),
tmp_cart as
(
    select
        user_id,
        count(*) cart_count,
        sum(cart_price*sku_num) cart_amount
    from dwd_fact_cart_info
    --分区后可以减少计算量 在分区内找数据
    where dt='2020-03-15'
and user_id is not null
--用户当天的行为，这是一个周期性快照事实全量表，所以要进行获取当天的时间，
and date_format(create_time,'yyyy-MM-dd')='2020-03-15'
    group by user_id
),
tmp_order as
(
    select
        user_id,
        count(*) order_count,
        sum(final_total_amount) order_amount
    from dwd_fact_order_info --累积型增量表 所以只获取新增的即可
    where dt='2020-03-15'
    group by user_id
) ,
tmp_payment as
(
    select
        user_id,
        count(*) payment_count,
        sum(payment_amount) payment_amount
    from dwd_fact_payment_info -- 增量表
    where dt='2020-03-15'
    group by user_id
)

insert overwrite table dws_user_action_daycount partition(dt='2020-03-15')
select
    --拼接法，union all后，group分组，然后sum(进行补位)这样就统计了一个用户一天的各个行为，为空的部分用0补    
    user_actions.user_id,
    sum(user_actions.login_count),
    sum(user_actions.cart_count),
    sum(user_actions.cart_amount),
    sum(user_actions.order_count),
    sum(user_actions.order_amount),
    sum(user_actions.payment_count),
    sum(user_actions.payment_amount)
from 
(
    select
        user_id,
        login_count,
        0 cart_count,
        0 cart_amount,
        0 order_count,
        0 order_amount,
        0 payment_count,
        0 payment_amount
    from 
    tmp_login
    union all
    select
        user_id,
        0 login_count,
        cart_count,
        cart_amount,
        0 order_count,
        0 order_amount,
        0 payment_count,
        0 payment_amount
    from 
    tmp_cart
    union all
    select
        user_id,
        0 login_count,
        0 cart_count,
        0 cart_amount,
        order_count,
        order_amount,
        0 payment_count,
        0 payment_amount
    from tmp_order
    union all
    select
        user_id,
        0 login_count,
        0 cart_count,
        0 cart_amount,
        0 order_count,
        0 order_amount,
        payment_count,
        payment_amount
    from tmp_payment
 ) user_actions
group by user_id;
===========================
会员主题表（全量的宽表）  dwt_user_topic
===========================
建表语句 根据DWD层的各种事实建表
===========================
drop table if exists dwt_user_topic;
create external table dwt_user_topic
(
    user_id string  comment '用户id',
    login_date_first string  comment '首次登录时间',
    login_date_last string  comment '末次登录时间',
    login_count bigint comment '累积登录天数',
    login_last_30d_count bigint comment '最近30日登录天数',
    order_date_first string  comment '首次下单时间',
    order_date_last string  comment '末次下单时间',
    order_count bigint comment '累积下单次数',
    order_amount decimal(16,2) comment '累积下单金额',
    order_last_30d_count bigint comment '最近30日下单次数',
    order_last_30d_amount bigint comment '最近30日下单金额',
    payment_date_first string  comment '首次支付时间',
    payment_date_last string  comment '末次支付时间',
    payment_count decimal(16,2) comment '累积支付次数',
    payment_amount decimal(16,2) comment '累积支付金额',
    payment_last_30d_count decimal(16,2) comment '最近30日支付次数',
    payment_last_30d_amount decimal(16,2) comment '最近30日支付金额'
 )COMMENT '用户主题宽表'
stored as parquet
location '/warehouse/gmall/dwt/dwt_user_topic/'
tblproperties ("parquet.compression"="lzo");
===========================
数据装载来自于老的dwt_user_topic以及dws_user_action_daycount;
===========================
insert overwrite table dwt_user_topic
select
    nvl(new.user_id,old.user_id),
    if(old.login_date_first is null and new.login_count>0,'2020-03-15',old.login_date_first),
    if(new.login_count>0,'2020-03-15',old.login_date_last),
    nvl(old.login_count,0)+if(new.login_count>0,1,0),
    nvl(new.login_last_30d_count,0),--直接在new中查到，只要你登陆了，就会增加，也就是你登陆必然是new
    if(old.order_date_first is null and new.order_count>0,'2020-03-15',old.order_date_first),
    if(new.order_count>0,'2020-03-15',old.order_date_last),
    nvl(old.order_count,0)+nvl(new.order_count,0),
    nvl(old.order_amount,0)+nvl(new.order_amount,0),
    nvl(new.order_last_30d_count,0),
    nvl(new.order_last_30d_amount,0),
    if(old.payment_date_first is null and new.payment_count>0,'2020-03-15',old.payment_date_first),
    if(new.payment_count>0,'2020-03-15',old.payment_date_last),
    nvl(old.payment_count,0)+nvl(new.payment_count,0),
    nvl(old.payment_amount,0)+nvl(new.payment_amount,0),
    nvl(new.payment_last_30d_count,0),
    nvl(new.payment_last_30d_amount,0)
from
--获取旧的全量表
dwt_user_topic old
full outer join
(
    select
    --获取三十天的全部每日用户行为数据从dws_user_action_daycount获取
    --以及单独得到当日的数据以用来求当日首次的那些数据
        user_id,
        --获取当日的数据
        sum(if(dt='2020-03-15',login_count,0)) login_count,
        sum(if(dt='2020-03-15',order_count,0)) order_count,
        sum(if(dt='2020-03-15',order_amount,0)) order_amount,
        sum(if(dt='2020-03-15',payment_count,0)) payment_count,
        sum(if(dt='2020-03-15',payment_amount,0)) payment_amount,
        --如果有登陆，则加1，加上三十天
        sum(if(login_count>0,1,0)) login_last_30d_count,
        --每次加的数据是支付数据
        sum(order_count) order_last_30d_count,
        sum(order_amount) order_last_30d_amount,
        sum(payment_count) payment_last_30d_count,
        sum(payment_amount) payment_last_30d_amount
    from dws_user_action_daycount
    --过滤三十天
    where dt>=date_add( '2020-03-15',-29)
    group by user_id
)new
on old.user_id=new.user_id;
===========================
每日商品行为（根据商品有关的事实表） dws_sku_action_daycount
===========================
建表语句
===========================
drop table if exists dws_sku_action_daycount;
create external table dws_sku_action_daycount 
(   
    sku_id string comment 'sku_id',
    order_count bigint comment '被下单次数',
    order_num bigint comment '被下单件数',
    order_amount decimal(16,2) comment '被下单金额',
    payment_count bigint  comment '被支付次数',
    payment_num bigint comment '被支付件数',
    payment_amount decimal(16,2) comment '被支付金额',
    refund_count bigint  comment '被退款次数',
    refund_num bigint comment '被退款件数',
    refund_amount  decimal(16,2) comment '被退款金额',
    cart_count bigint comment '被加入购物车次数',
    cart_num bigint comment '被加入购物车件数',
    favor_count bigint comment '被收藏次数',
    appraise_good_count bigint comment '好评数',
    appraise_mid_count bigint comment '中评数',
    appraise_bad_count bigint comment '差评数',
    appraise_default_count bigint comment '默认评价数'
) COMMENT '每日商品行为'
PARTITIONED BY (`dt` string)
stored as parquet
location '/warehouse/gmall/dws/dws_sku_action_daycount/'
tblproperties ("parquet.compression"="lzo");
===========================
数据装载来自于dwd_fact_order_detail，dwd_fact_order_info，dwd_fact_order_refund_info
dwd_fact_cart_info  dwd_fact_favor_info dwd_fact_comment_info
===========================
with 
tmp_order as
(   --根据商品skuid进行分组获取到下单次数，下单数量，下单的金额
    select
        sku_id,
        count(*) order_count,
        sum(sku_num) order_num,
        sum(total_amount) order_amount
    from dwd_fact_order_detail
    where dt='2020-03-15'
    group by sku_id
),
tmp_payment as
(   --根据dwd_fact_order_detail获取支付信息，支付的次数，支付的简述
    select
        sku_id,
        count(*) payment_count,
        sum(sku_num) payment_num,
        sum(total_amount) payment_amount
    from dwd_fact_order_detail
    where dt='2020-03-15'
    and order_id in
    (   --从dwd_fact_order_info累积型快照事实表获取当日有支付的订单id
        select
            id
        from dwd_fact_order_info
        where (dt='2020-03-15'
        or dt=date_add('2020-03-15',-1))--支付有可能是第二天晚上支付，分区到前一晚
        and date_format(payment_time,'yyyy-MM-dd')='2020-03-15'--支付时间
    )
    group by sku_id
),
tmp_refund as
(
    select
        sku_id,
        count(*) refund_count,
        sum(refund_num) refund_num,
        sum(refund_amount) refund_amount
    from dwd_fact_order_refund_info
    where dt='2020-03-15'--事务型事实表直接增加
    group by sku_id
),
tmp_cart as
(
    select
        sku_id,
        count(*) cart_count,
        sum(sku_num) cart_num
    from dwd_fact_cart_info
    where dt='2020-03-15'
    and date_format(create_time,'yyyy-MM-dd')='2020-03-15'--获取创建时间
    group by sku_id
),
tmp_favor as
(
    select
        sku_id,
        count(*) favor_count
    from dwd_fact_favor_info
    where dt='2020-03-15'
    and date_format(create_time,'yyyy-MM-dd')='2020-03-15'
    group by sku_id
),
tmp_appraise as
(
select
    sku_id,
    sum(if(appraise='1201',1,0)) appraise_good_count,
    sum(if(appraise='1202',1,0)) appraise_mid_count,
    sum(if(appraise='1203',1,0)) appraise_bad_count,
    sum(if(appraise='1204',1,0)) appraise_default_count
from dwd_fact_comment_info
where dt='2020-03-15'
group by sku_id
)

insert overwrite table dws_sku_action_daycount partition(dt='2020-03-15')
select
    sku_id,
    sum(order_count),
    sum(order_num),
    sum(order_amount),
    sum(payment_count),
    sum(payment_num),
    sum(payment_amount),
    sum(refund_count),
    sum(refund_num),
    sum(refund_amount),
    sum(cart_count),
    sum(cart_num),
    sum(favor_count),
    sum(appraise_good_count),
    sum(appraise_mid_count),
    sum(appraise_bad_count),
    sum(appraise_default_count)
from
(
    select
        sku_id,
        order_count,
        order_num,
        order_amount,
        0 payment_count,
        0 payment_num,
        0 payment_amount,
        0 refund_count,
        0 refund_num,
        0 refund_amount,
        0 cart_count,
        0 cart_num,
        0 favor_count,
        0 appraise_good_count,
        0 appraise_mid_count,
        0 appraise_bad_count,
        0 appraise_default_count
    from tmp_order
    union all
    select
        sku_id,
        0 order_count,
        0 order_num,
        0 order_amount,
        payment_count,
        payment_num,
        payment_amount,
        0 refund_count,
        0 refund_num,
        0 refund_amount,
        0 cart_count,
        0 cart_num,
        0 favor_count,
        0 appraise_good_count,
        0 appraise_mid_count,
        0 appraise_bad_count,
        0 appraise_default_count
    from tmp_payment
    union all
    select
        sku_id,
        0 order_count,
        0 order_num,
        0 order_amount,
        0 payment_count,
        0 payment_num,
        0 payment_amount,
        refund_count,
        refund_num,
        refund_amount,
        0 cart_count,
        0 cart_num,
        0 favor_count,
        0 appraise_good_count,
        0 appraise_mid_count,
        0 appraise_bad_count,
        0 appraise_default_count        
    from tmp_refund
    union all
    select
        sku_id,
        0 order_count,
        0 order_num,
        0 order_amount,
        0 payment_count,
        0 payment_num,
        0 payment_amount,
        0 refund_count,
        0 refund_num,
        0 refund_amount,
        cart_count,
        cart_num,
        0 favor_count,
        0 appraise_good_count,
        0 appraise_mid_count,
        0 appraise_bad_count,
        0 appraise_default_count
    from tmp_cart
    union all
    select
        sku_id,
        0 order_count,
        0 order_num,
        0 order_amount,
        0 payment_count,
        0 payment_num,
        0 payment_amount,
        0 refund_count,
        0 refund_num,
        0 refund_amount,
        0 cart_count,
        0 cart_num,
        favor_count,
        0 appraise_good_count,
        0 appraise_mid_count,
        0 appraise_bad_count,
        0 appraise_default_count
    from tmp_favor
    union all
    select
        sku_id,
        0 order_count,
        0 order_num,
        0 order_amount,
        0 payment_count,
        0 payment_num,
        0 payment_amount,
        0 refund_count,
        0 refund_num,
        0 refund_amount,
        0 cart_count,
        0 cart_num,
        0 favor_count,
        appraise_good_count,
        appraise_mid_count,
        appraise_bad_count,
        appraise_default_count
    from tmp_appraise
)tmp
group by sku_id;
===========================
商品主题表（概念来自DWD层维度表，根据DWD事实表建表，数据来自DWS每日活跃）dwt_sku_topic
===========================
建表语句
===========================
drop table if exists dwt_sku_topic;
create external table dwt_sku_topic
(
    sku_id string comment 'sku_id',
    spu_id string comment 'spu_id',
    order_last_30d_count bigint comment '最近30日被下单次数',
    order_last_30d_num bigint comment '最近30日被下单件数',
    order_last_30d_amount decimal(16,2)  comment '最近30日被下单金额',
    order_count bigint comment '累积被下单次数',
    order_num bigint comment '累积被下单件数',
    order_amount decimal(16,2) comment '累积被下单金额',
    payment_last_30d_count   bigint  comment '最近30日被支付次数',
    payment_last_30d_num bigint comment '最近30日被支付件数',
    payment_last_30d_amount  decimal(16,2) comment '最近30日被支付金额',
    payment_count   bigint  comment '累积被支付次数',
    payment_num bigint comment '累积被支付件数',
    payment_amount  decimal(16,2) comment '累积被支付金额',
    refund_last_30d_count bigint comment '最近三十日退款次数',
    refund_last_30d_num bigint comment '最近三十日退款件数',
    refund_last_30d_amount decimal(10,2) comment '最近三十日退款金额',
    refund_count bigint comment '累积退款次数',
    refund_num bigint comment '累积退款件数',
    refund_amount decimal(10,2) comment '累积退款金额',
    cart_last_30d_count bigint comment '最近30日被加入购物车次数',
    cart_last_30d_num bigint comment '最近30日被加入购物车件数',
    cart_count bigint comment '累积被加入购物车次数',
    cart_num bigint comment '累积被加入购物车件数',
    favor_last_30d_count bigint comment '最近30日被收藏次数',
    favor_count bigint comment '累积被收藏次数',
    appraise_last_30d_good_count bigint comment '最近30日好评数',
    appraise_last_30d_mid_count bigint comment '最近30日中评数',
    appraise_last_30d_bad_count bigint comment '最近30日差评数',
    appraise_last_30d_default_count bigint comment '最近30日默认评价数',
    appraise_good_count bigint comment '累积好评数',
    appraise_mid_count bigint comment '累积中评数',
    appraise_bad_count bigint comment '累积差评数',
    appraise_default_count bigint comment '累积默认评价数'
 )COMMENT '商品主题宽表'
stored as parquet
location '/warehouse/gmall/dwt/dwt_sku_topic/'
tblproperties ("parquet.compression"="lzo");
===========================
数据装载来自于每日商品行为新的dws_sku_action_daycount，老的dwt_sku_topic
===========================
with
sku_act as
(
select
    --从dws_sku_action_daycount获取日活跃商品的行为
    sku_id,
    sum(if(dt='2020-03-15', order_count,0 )) order_count,
    sum(if(dt='2020-03-15',order_num ,0 ))  order_num, 
    sum(if(dt='2020-03-15',order_amount,0 )) order_amount ,
    sum(if(dt='2020-03-15',payment_count,0 )) payment_count,
    sum(if(dt='2020-03-15',payment_num,0 )) payment_num,
    sum(if(dt='2020-03-15',payment_amount,0 )) payment_amount,
    sum(if(dt='2020-03-15',refund_count,0 )) refund_count,
    sum(if(dt='2020-03-15',refund_num,0 )) refund_num,
    sum(if(dt='2020-03-15',refund_amount,0 )) refund_amount,  
    sum(if(dt='2020-03-15',cart_count,0 )) cart_count,
    sum(if(dt='2020-03-15',cart_num,0 )) cart_num,
    sum(if(dt='2020-03-15',favor_count,0 )) favor_count,
    sum(if(dt='2020-03-15',appraise_good_count,0 )) appraise_good_count,  
    sum(if(dt='2020-03-15',appraise_mid_count,0 ) ) appraise_mid_count ,
    sum(if(dt='2020-03-15',appraise_bad_count,0 )) appraise_bad_count,  
    sum(if(dt='2020-03-15',appraise_default_count,0 )) appraise_default_count,
    sum(order_count  ) order_count30 ,
    sum(order_num  )  order_num30,
    sum(order_amount ) order_amount30,
    sum(payment_count ) payment_count30,
    sum(payment_num ) payment_num30,
    sum(payment_amount ) payment_amount30,
    sum(refund_count  ) refund_count30,
    sum(refund_num ) refund_num30,
    sum(refund_amount ) refund_amount30,
    sum(cart_count  ) cart_count30,
    sum(cart_num ) cart_num30,
    sum(favor_count ) favor_count30,
    sum(appraise_good_count ) appraise_good_count30,
    sum(appraise_mid_count  ) appraise_mid_count30,
    sum(appraise_bad_count ) appraise_bad_count30,
    sum(appraise_default_count )  appraise_default_count30 
from dws_sku_action_daycount
where dt>=date_add ( '2020-03-15',-29)
group by sku_id
),
--商品主题老数据
sku_topic
as 
(
select
    sku_id,
    spu_id,
    order_last_30d_count,
    order_last_30d_num,
    order_last_30d_amount,
    order_count,
    order_num,
    order_amount  ,
    payment_last_30d_count,
    payment_last_30d_num,
    payment_last_30d_amount,
    payment_count,
    payment_num,
    payment_amount,
    refund_last_30d_count,
    refund_last_30d_num,
    refund_last_30d_amount ,
    refund_count  ,
    refund_num ,
    refund_amount  ,
    cart_last_30d_count  ,
    cart_last_30d_num  ,
    cart_count  ,
    cart_num  ,
    favor_last_30d_count  ,
    favor_count  ,
    appraise_last_30d_good_count  ,
    appraise_last_30d_mid_count  ,
    appraise_last_30d_bad_count  ,
    appraise_last_30d_default_count  ,
    appraise_good_count  ,
    appraise_mid_count  ,
    appraise_bad_count  ,
    appraise_default_count 
from dwt_sku_topic
)
insert overwrite table dwt_sku_topic
select 
    nvl(sku_act.sku_id,sku_topic.sku_id) ,
    dwd_dim_sku_info.spu_id,
    nvl (sku_act.order_count30,0)      ,
    nvl (sku_act.order_num30,0)   ,
    nvl (sku_act.order_amount30,0)   ,
    nvl(sku_topic.order_count,0)+ nvl (sku_act.order_count,0) ,
    nvl(sku_topic.order_num,0)+ nvl (sku_act.order_num,0)   ,
    nvl(sku_topic.order_amount,0)+ nvl (sku_act.order_amount,0),
    nvl (sku_act.payment_count30,0),
    nvl (sku_act.payment_num30,0),
    nvl (sku_act.payment_amount30,0),
    nvl(sku_topic.payment_count,0)+ nvl (sku_act.payment_count,0) ,
    nvl(sku_topic.payment_num,0)+ nvl (sku_act.payment_count,0)  ,
    nvl(sku_topic.payment_amount,0)+ nvl (sku_act.payment_count,0)  ,
    nvl (sku_act.refund_count30,0),
    nvl (sku_act.refund_num30,0),
    nvl (sku_act.refund_amount30,0),
    nvl(sku_topic.refund_count,0)+ nvl (sku_act.refund_count,0),
    nvl(sku_topic.refund_num,0)+ nvl (sku_act.refund_num,0),
    nvl(sku_topic.refund_amount,0)+ nvl (sku_act.refund_amount,0),
    nvl(sku_act.cart_count30,0)  ,
    nvl(sku_act.cart_num30,0)  ,
    nvl(sku_topic.cart_count  ,0)+ nvl (sku_act.cart_count,0),
    nvl( sku_topic.cart_num  ,0)+ nvl (sku_act.cart_num,0),
    nvl(sku_act.favor_count30 ,0)  ,
    nvl (sku_topic.favor_count  ,0)+ nvl (sku_act.favor_count,0),
    nvl (sku_act.appraise_good_count30 ,0)  ,
    nvl (sku_act.appraise_mid_count30 ,0)  ,
    nvl (sku_act.appraise_bad_count30 ,0)  ,
    nvl (sku_act.appraise_default_count30 ,0)  ,
    nvl (sku_topic.appraise_good_count  ,0)+ nvl (sku_act.appraise_good_count,0)  ,
    nvl (sku_topic.appraise_mid_count   ,0)+ nvl (sku_act.appraise_mid_count,0) ,
    nvl (sku_topic.appraise_bad_count  ,0)+ nvl (sku_act.appraise_bad_count,0)  ,
    nvl (sku_topic.appraise_default_count  ,0)+ nvl (sku_act.appraise_default_count,0) 
from sku_act
full outer join sku_topic
on sku_act.sku_id =sku_topic.sku_id
left join dwd_dim_sku_info--获取商品的名称
on sku_act.sku_id = dwd_dim_sku_info.id;
===========================
每日优惠券表（每日日活） dws_coupon_use_daycount
===========================
建表语句
===========================
drop table if exists dws_coupon_use_daycount;
create external table dws_coupon_use_daycount
(   
    `coupon_id` string  COMMENT '优惠券ID',
    `coupon_name` string COMMENT '购物券名称',
    `coupon_type` string COMMENT '购物券类型 1 现金券 2 折扣券 3 满减券 4 满件打折券',
    `condition_amount` string COMMENT '满额数',
    `condition_num` string COMMENT '满件数',
    `activity_id` string COMMENT '活动编号',
    `benefit_amount` string COMMENT '减金额',
    `benefit_discount` string COMMENT '折扣',
    `create_time` string COMMENT '创建时间',
    `range_type` string COMMENT '范围类型 1、商品 2、品类 3、品牌',
    `spu_id` string COMMENT '商品id',
    `tm_id` string COMMENT '品牌id',
    `category3_id` string COMMENT '品类id',
    `limit_num` string COMMENT '最多领用次数',
    `get_count` bigint COMMENT '领用次数',
    `using_count` bigint COMMENT '使用(下单)次数',
    `used_count` bigint COMMENT '使用(支付)次数'
) COMMENT '每日优惠券统计'
PARTITIONED BY (`dt` string)
stored as parquet
location '/warehouse/gmall/dws/dws_coupon_use_daycount/'
tblproperties ("parquet.compression"="lzo");
===========================
数据装载来自dwd_fact_couponuse以及dwd_dim_coupon_info  从事实表获取次数，从维度表获取信息
===========================
insert overwrite table dws_coupon_use_daycount partition(dt='2020-03-15')
select
    cu.coupon_id,
    ci.coupon_name,
    ci.coupon_type,
    ci.condition_amount,
    ci.condition_num,
    ci.activity_id,
    ci.benefit_amount,
    ci.benefit_discount,
    ci.create_time,
    ci.range_type,
    ci.spu_id,
    ci.tm_id,
    ci.category3_id,
    ci.limit_num,
    cu.get_count,
    cu.using_count,
    cu.used_count
from 
(   --获取次数是从事实表中
    select
        coupon_id,
        sum(if(date_format(get_time,'yyyy-MM-dd')='2020-03-15',1,0)) get_count,
        sum(if(date_format(using_time,'yyyy-MM-dd')='2020-03-15',1,0)) using_count,
        sum(if(date_format(used_time,'yyyy-MM-dd')='2020-03-15',1,0)) used_count
    from dwd_fact_coupon_use
    where dt='2020-03-15'
    group by coupon_id
)cu
left join
(   --左外获取优惠券的信息
    select
        *
    from dwd_dim_coupon_info--事物表（全量）
    where dt='2020-03-15'
)ci on cu.coupon_id=ci.id;

===========================
优惠券主题表（全量） dwt_coupon_topic
===========================
建表语句
===========================
drop table if exists dwt_coupon_topic;
create external table dwt_coupon_topic
(
    `coupon_id` string  COMMENT '优惠券ID',
    `get_day_count` bigint COMMENT '当日领用次数',
    `using_day_count` bigint COMMENT '当日使用(下单)次数',
    `used_day_count` bigint COMMENT '当日使用(支付)次数',
    `get_count` bigint COMMENT '累积领用次数',
    `using_count` bigint COMMENT '累积使用(下单)次数',
    `used_count` bigint COMMENT '累积使用(支付)次数'
)COMMENT '购物券主题宽表'
stored as parquet
location '/warehouse/gmall/dwt/dwt_coupon_topic/'
tblproperties ("parquet.compression"="lzo");
===========================
数据装载来自新的dws_coupon_use_daycount以及老的dwt_coupon_topic
===========================
insert overwrite table dwt_coupon_topic
select
    nvl(new.coupon_id,old.coupon_id),
    nvl(new.get_count,0),
    nvl(new.using_count,0),
    nvl(new.used_count,0),
    nvl(old.get_count,0)+nvl(new.get_count,0),
    nvl(old.using_count,0)+nvl(new.using_count,0),
    nvl(old.used_count,0)+nvl(new.used_count,0)
from
(
    select
        *
    from dwt_coupon_topic
)old
full outer join
(
    select
        coupon_id,
        get_count,
        using_count,
        used_count
    from dws_coupon_use_daycount
    where dt='2020-03-15'
)new
on old.coupon_id=new.coupon_id;
===========================
每日活动信息（建表来自维度表，数据来自事实表）dws_activity_info_daycount
===========================
建表语句
===========================
drop table if exists dws_activity_info_daycount;
create external table dws_activity_info_daycount(
    `id` string COMMENT '编号',
    `activity_name` string  COMMENT '活动名称',
    `activity_type` string  COMMENT '活动类型',
    `start_time` string  COMMENT '开始时间',
    `end_time` string  COMMENT '结束时间',
    `create_time` string  COMMENT '创建时间',
    `order_count` bigint COMMENT '下单次数',
    `payment_count` bigint COMMENT '支付次数'
) COMMENT '购物车信息表'
PARTITIONED BY (`dt` string)
row format delimited fields terminated by '\t'
location '/warehouse/gmall/dws/dws_activity_info_daycount/'
tblproperties ("parquet.compression"="lzo");
===========================
数据装载来自dwd_dim_activity_info获取活动信息，dwd_fact_order_info获取数据
===========================
insert overwrite table dws_activity_info_daycount partition(dt='2020-03-15')
select
    oi.activity_id,
    ai.activity_name,
    ai.activity_type,
    ai.start_time,
    ai.end_time,
    ai.create_time,
    oi.order_count,
    oi.payment_count
from
(
    select
        activity_id,
        sum(if(date_format(create_time,'yyyy-MM-dd')='2020-03-10',1,0)) order_count,
        sum(if(date_format(payment_time,'yyyy-MM-dd')='2020-03-10',1,0)) payment_count
    from dwd_fact_order_info
    --因为是累积型快照事实表所以按照的是createtime分区，有可能支付创建出现在前一天，支付完成在后一天，支付有可能是第二天晚上支付，分区到前一晚
    where (dt='2020-03-15' or dt=date_add('2020-03-15',-1))
    and activity_id is not null
    group by activity_id
)oi
join
(
    select
        *
    from dwd_dim_activity_info
    where dt='2020-03-15'
)ai
on oi.activity_id=ai.id;
===========================
活动主题宽表（全量） dwt_activity_topic
===========================
建表语句
===========================
drop table if exists dwt_activity_topic;
create external table dwt_activity_topic(
    `id` string COMMENT '活动id',
    `activity_name` string  COMMENT '活动名称',
    `order_day_count` bigint COMMENT '当日日下单次数',
    `payment_day_count` bigint COMMENT '当日支付次数',
    `order_count` bigint COMMENT '累积下单次数',
    `payment_count` bigint COMMENT '累积支付次数'
) COMMENT '活动主题宽表'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/dwt/dwt_activity_topic/'
tblproperties ("parquet.compression"="lzo");
===========================
数据装载来自于dwt_activity_topic，新的dws_activity_info_daycount
===========================
insert overwrite table dwt_activity_topic
select
    nvl(new.id,old.id),
    nvl(new.activity_name,old.activity_name),
    nvl(new.order_count,0),
    nvl(new.payment_count,0),
    nvl(old.order_count,0)+nvl(new.order_count,0),
    nvl(old.payment_count,0)+nvl(new.payment_count,0)
from
(
    select
        *
    from dwt_activity_topic
)old
full outer join
(
    select
        id,
        activity_name,
        order_count,
        payment_count
    from dws_activity_info_daycount
    where dt='2020-03-15'
)new
on old.id=new.id;






==============================================================================================================================================
--------------------------------------------------------ADS层---------------------------------------------------------------------------------
=======================================================需求层====================================================================================
----------------------------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------




===========================
设备主题需求
===========================
活跃设备数 (日，周，月)
日活：当日活跃的设备数
周活：当周活跃的设备数
月活：当月活跃的设备数
===========================
建表语句
===========================
drop table if exists ads_uv_count;
create external table ads_uv_count( 
`dt` string COMMENT '统计日期',
`day_count` bigint COMMENT '当日用户数量',
`wk_count`  bigint COMMENT '当周用户数量',
`mn_count`  bigint COMMENT '当月用户数量',
`is_weekend` string COMMENT 'Y,N是否是周末,用于得到本周最终结果',
`is_monthend` string COMMENT 'Y,N是否是月末,用于得到本月最终结果' 
) COMMENT '活跃设备数'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_uv_count/';
===========================
数据装载 来自于dwt_uv_topic
===========================
--获取当日的活跃数量
select
'2020-03-15',
count(*) day_count
from dwt_uv_topic
where login_date_last=date_format('2020-03-15','yyyy-MM-dd');
--获取当周活跃数量
select
count(*) week_count
from dwt_uv_topic
where login_date_last>=date_add(next_day('2020-03-15','MO'),-7) and login_date_last<=date_add(next_day('2020-03-15','MO'),-1);
--获取当月活跃数量
select
'2020-03-15',
count(*) month_count
from dwt_uv_topic
where date_format('2020-03-15','yyyy-MM') = date_format(login_date_last,'yyyy-MM');

--不用管去重，因为过滤的时候选取的就是最后一次活跃，一个id最后一次活跃就是唯一确定的
insert into table ads_uv_count 
select
  '2020-03-15' ,
  day.day_count,
  week.week_count,
  month.month_count,
  if (date_format(login_date_last,"yyyy-MM-dd")== date_add(next_day(login_date_last,"MO"),-1),"y","n"),
  if('2020-03-15'=last_day('2020-03-15'),'Y','N')
from
(select
'2020-03-15' dt,
count(*) day_count
from dwt_uv_topic
where login_date_last=date_format('2020-03-15','yyyy-MM-dd')
) day
join 
(select
'2020-03-15' dt,
count(*) week_count
from dwt_uv_topic
where login_date_last>=date_add(next_day('2020-03-15','MO'),-7) and login_date_last<=date_add(next_day('2020-03-15','MO'),-1)
)week
on day.dt = week.dt
join
(select
'2020-03-15' dt,
count(*) month_count
from dwt_uv_topic
where date_format('2020-03-15','yyyy-MM') = date_format(login_date_last,'yyyy-MM')
)month
on day.dt = month.dt;


===========================
每日新增设备 ads_new_mid_count
===========================
建表语句
===========================
drop table if exists ads_new_mid_count;
create external table ads_new_mid_count
(
`create_date` string comment '创建时间' ,
`new_mid_count` BIGINT comment '新增设备数量' 
)  COMMENT '每日新增设备信息数量'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_new_mid_count/';
===========================
数据装载 来自于dwt_uv_topic
===========================
insert into table ads_new_mid_count
select
'2020-03-15',
count(*)
from dwt_uv_topic
where login_date_first = '2020-03-15';
===========================
沉默用户数量 ads_silent_count
===========================
建表语句 只在安装当天启动过一次，且启动时间是在7天前
===========================
drop table if exists ads_silent_count;
create external table ads_silent_count( 
`dt` string COMMENT '统计日期',
`silent_count` bigint COMMENT '沉默设备数'
) 
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_silent_count';
===========================
数据装载 数据来自于dwt_uv_topic
===========================
insert into table ads_silent_count
select
'2020-03-15',
count(*)
from dwt_uv_topic
where login_date_last<=date_format(date_add('2020-03-15',-7),'yyyy-MM-dd') and login_count=1; --累计活动天数为一天
===========================
本周回流用户  ads_back_count
===========================
建表语句
===========================
drop table if exists ads_back_count;
create external table ads_back_count( 
    `dt` string COMMENT '统计日期',
    `wk_dt` string COMMENT '统计日期所在周',
    `wastage_count` bigint COMMENT '回流设备数'
) 
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_back_count';

===========================
回流用户，上周未活跃，本周活跃的设备，且不是本周新增设备   数据来自于dws_uv_detail_daycount，dwt_uv_topic
===========================
--思路：
--统计出本周活跃的设备，以及上周活跃的设备，对比，取出上周活跃的设备，就可以得到上周未活跃的部分，同时减去本周新增的设备

select
--获取本周活跃且不包含新设备
mid_id
from dwt_uv_topic
--过滤出本周活跃的设备
where login_date_last >= date_add(next_day('2020-03-15','MO'),-7) 
and login_date_last <= date_add(next_day('2020-03-15','MO'),-1)
--过滤新增的数据 
and login_date_first < date_add(next_day('2020-03-15','MO'),-7)

--获取上个周活跃的设备只能从日活表中取
select
mid_id
from dwt_uv_detail_daycount
where dt > date_add(next_day('2020-03-15','MO'),-7*2) and login_date_last <= date_add(next_day('2020-03-15','MO'),-8)
group by mid_id;


insert into table ads_back_count
select
'2020-03-15',
 weekofyear('2020-03-15'),
 count(*)
from 
(
  select
  --获取本周活跃且不包含新设备
  mid_id
  from dwt_uv_topic
  --过滤出本周活跃的设备
  where login_date_last >= date_add(next_day('2020-03-15','MO'),-7) 
  and login_date_last <= date_add(next_day('2020-03-15','MO'),-1)
  --过滤新增的数据 
  and login_date_first < date_add(next_day('2020-03-15','MO'),-7)
)t1
left join(
  --获取前前一周数据只能从日活表中获取
  select
  mid_id
  from dws_uv_detail_daycount
  where dt >= date_add(next_day('2020-03-15','MO'),-7*2) and dt <= date_add(next_day('2020-03-15','MO'),-8) 
  group by mid_id
)t2
on t1.mid_id = t2.mid_id
where t2.mid_id is null;


--问题  这个能获取前一周的活跃数据吗？ ，如果不可以为什么？
--回答：对于，上周活跃并且 本周也活跃的用户，这样获取不到的
--      你这样获取的是，上周活跃，本周没有活跃的用户 所以不能找出本周是否活跃

select
mid_id
from dwt_uv_topic
where login_date_last >= date_add(next_day('2020-03-15','MO'),-7*2) 
and login_date_last <= date_add(next_day('2020-03-15','MO'),-8)


===========================
流失用户数 ads_wastage_count
===========================
建表语句  流失用户：连续7天未活跃的设备
===========================
drop table if exists ads_wastage_count;
create external table ads_wastage_count( 
    `dt` string COMMENT '统计日期',
    `wastage_count` bigint COMMENT '流失设备数'
) 
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_wastage_count';
===========================
数据装载 来自于dwt_uv_topic
===========================
insert into table ads_wastage_count
select
'2020-03-15',
count(*)
from dwt_uv_topic
--换了个角度
where login_date_last<=date_add('2020-03-15',-7);
===========================
留存率 ads_user_retention_day_rate
===========================
建表语句
===========================
drop table if exists ads_user_retention_day_rate;
create external table ads_user_retention_day_rate 
(
`stat_date` string comment '统计日期',
`create_date` string  comment '设备新增日期',
`retention_day` int comment '截止当前日期留存天数',
`retention_count` bigint comment  '留存数量',
`new_mid_count` bigint comment '设备新增数量',
`retention_ratio` decimal(10,2) comment '留存率'
)  COMMENT '每日用户留存情况'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_user_retention_day_rate/';
===========================
数据装载 数据来自于dwt_uv_topic
===========================
--分析
比如需要算 2020-03-12 一日留存
需要 2020-03-12 的新增 以及 2020-03-13 的活跃  但是拿 03-13 的活跃 只能 14 号才能拿到
          2020-03-12 一日留存
  需要 2020-03-12 的新增 以及 2020-03-14 的活跃 但是拿 03-14 的活跃 只能 15 号才能拿到
          2020-03-12 一日留存 
  需要 2020-03-12 的新增 以及 2020-03-15 的活跃  但是拿 03-15 的活跃 只能 16 号才能拿到      

--确定一天的例子
实际上计算的是 2020-03-16 一天的数据
2020-03-12 的三日留存
2020-03-13 的二日留存
2020-03-14 的一日留存


2020-03-12 的三日留存
select
'2020-03-16',
'2020-03-12',
3,
--找到2020-03-12新增用户，并且在15号活跃的用户
sum(if(login_date_first='2020-03-12' and login_date_last='2020-03-15',1,0)),
sum(if(login_date_first='2020-03-12',1,0)),
sum(if(login_date_first='2020-03-12' and login_date_last='2020-03-15',1,0))/sum(if(login_date_first='2020-03-12',1,0))*100
from dwt_uv_topic;
2020-03-13 的二日留存
select
'2020-03-16',
'2020-03-13',
2,
--找到2020-03-13新增用户，并且在15号活跃的用户
sum(if(login_date_first='2020-03-13' and login_date_last='2020-03-15',1,0)),
sum(if(login_date_first='2020-03-13',1,0)),
sum(if(login_date_first='2020-03-13' and login_date_last='2020-03-15',1,0))/sum(if(login_date_first='2020-03-13',1,0))*100
2020-03-14 的一日留存
select
'2020-03-16',
'2020-03-14',
1,
--找到2020-03-14新增用户，并且在15号活跃的用户
sum(if(login_date_first='2020-03-14' and login_date_last='2020-03-15',1,0)),
sum(if(login_date_first='2020-03-14',1,0))
sum(if(login_date_first='2020-03-14' and login_date_last='2020-03-15',1,0))/sum(if(login_date_first='2020-03-14',1,0))*100

综上再unionall一下

写一个脚本
#!/bin/bash/
$do_date=前一天的日期 比如说是 2020-03-15
insert into table ads_user_retention_day_rate
select
date_add('$do_date',1),
date_add('$do_date',-3),
3,
sum(if(login_date_first=date_add('$do_date',-3) and login_date_last='$do_date',1,0)),
sum(if(login_date_first=date_add('$do_date',-3),1,0)),
sum(if(login_date_first=date_add('$do_date',-3) and login_date_last='$do_date',1,0))/sum(if(login_date_first=date_add('$do_date',-3),1,0))*100
from dwt_uv_topic
union all
select
date_add('$do_date',1),
date_add('$do_date',-2),
2,
--找到2020-03-13新增用户，并且在15号活跃的用户
sum(if(login_date_first=date_add('$do_date',-2) and login_date_last='$do_date',1,0)),
sum(if(login_date_first=date_add('$do_date',-2),1,0)),
sum(if(login_date_first=date_add('$do_date',-2) and login_date_last='$do_date',1,0))/sum(if(login_date_first=date_add('$do_date',-2),1,0))*100
from dwt_uv_topic
union all
select
date_add('$do_date',1),
date_add('$do_date',-1),
1,
--找到2020-03-14新增用户，并且在15号活跃的用户
sum(if(login_date_first=date_add('$do_date',-1) and login_date_last='$do_date',1,0)),
sum(if(login_date_first=date_add('$do_date',-1),1,0)),
sum(if(login_date_first=date_add('$do_date',-1) and login_date_last='$do_date',1,0))/sum(if(login_date_first=date_add('$do_date',-1),1,0))*100
from dwt_uv_topic;


===========================




最近三周活跃用户 ads_continuity_wk_count
===========================
建表语句
===========================
drop table if exists ads_continuity_wk_count;
create external table ads_continuity_wk_count( 
    `dt` string COMMENT '统计日期,一般用结束周周日日期,如果每天计算一次,可用当天日期',
    `wk_dt` string COMMENT '持续时间',
    `continuity_count` bigint COMMENT '连续活跃人数'
) 
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_continuity_wk_count';
===========================
数据装载 数据来自于dws_uv_detail_daycount 数据只能从日活获取
===========================
insert into table ads_continuity_wk_count
select
'2020-03-15',
concat(date_add(next_day('2020-03-15','MO'),-7*3),'->',date_add(next_day('2020-03-15','SUN'),-7)),
--注意要在外层进行count(*)
count(*)
from
(
  select
  mid_id
  from
  (
  select
  '2020-03-15',
  mid_id,
  sum(if(login_count>0,1,0))
  from dws_uv_detail_daycount
  --本周
  where dt >=date_add(next_day('2020-03-15','MO'),-7) and dt <= date_add(next_day('2020-03-15','SUN'),-7)
  group by mid_id
  union all
  select
  '2020-03-15',
  mid_id,
  sum(if(login_count>0,1,0))
  from dws_uv_detail_daycount
  --上周
  where dt >=date_add(next_day('2020-03-15','MO'),-7*2) and dt <= date_add(next_day('2020-03-15','SUN'),-7*2)
  group by mid_id
  union all
  select
  '2020-03-15',
  mid_id,
  sum(if(login_count>0,1,0))
  from dws_uv_detail_daycount
  --上上周
  where dt >=date_add(next_day('2020-03-15','MO'),-7*3) and dt <= date_add(next_day('2020-03-15','SUN'),-7*3)
  group by mid_id
  )t1
  group by mid_id
  having count(*)=3
)t2;
===========================
最近七天内连续三天活跃用户数
===========================
建表语句
===========================
drop table if exists ads_continuity_uv_count;
create external table ads_continuity_uv_count( 
    `dt` string COMMENT '统计日期',
    `wk_dt` string COMMENT '最近7天日期',
    `continuity_count` bigint --连续活跃人数
) COMMENT '连续活跃设备数'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_continuity_uv_count';
===========================
数据装载来自于dws_uv_detail_daycount
===========================
mid_id   dt          rank     dt_diff
1      2020-3-11      1       2020-03-10
1      2020-3-12      2       2020-03-10
1      2020-3-13      3       2020-03-10

1      2020-3-14      4       2020-03-11
1      2020-3-15      5       2020-03-11
1      2020-3-16      6       2020-03-11

2      2020-3-11
2      2020-3-12
2      2020-3-13
2      2020-3-14
2      2020-3-15
2      2020-3-16


--获取前七天的数据，日期，排名
select
mid_id,
dt,
rank() over(partition by mid_id order by dt) r
from dws_uv_detail_daycount
where dt >= date_add('2020-03-15',-6) and dt <= '2020-03-15';

--做减法
select
mid_id,
date_add(dt,-r) dt_diff
from 
(
  select
    mid_id,
    dt,
    rank() over(partition by mid_id order by dt) r
  from dws_uv_detail_daycount
  where dt >= date_add('2020-03-15',-6) and dt <= '2020-03-15'
)t1;


select
mid_id
from
(
    select
    mid_id
  from
  (
    select
      mid_id,
      date_add(dt,-r) dt_diff
    from
    (
      select
        mid_id,
        dt,
        rank() over(partition by mid_id order by dt) r
      from dws_uv_detail_daycount
      where dt >= date_add('2020-03-15',-6) and dt <= '2020-03-15'
    )t1
  )t2
  --分组加过滤
  --重点分组时候一定要选取mid+dt_diff
  group by mid_id,dt_diff
  having count(*) >= 3
)t3
--去重  mid相同 但是时间不同的数据 时间
group by mid_id;


insert into table ads_continuity_uv_count
select
    '2020-03-15',
    concat(date_add('2020-03-15',-6),'_','2020-03-15'),
    count(*)
from
(
  select
mid_id
from
(
    select
      mid_id
    from
    (
      select
        mid_id,
        date_add(dt,-r) dt_diff
      from
      (
        select
          mid_id,
          dt,
          rank() over(partition by mid_id order by dt) r
        from dws_uv_detail_daycount
        where dt >= date_add('2020-03-15',-6) and dt <= '2020-03-15'
      )t1
    )t2
    --分组加过滤
    --重点分组时候一定要选取mid+dt_diff
    group by mid_id,dt_diff
    having count(*) >= 3
  )t3
  --去重  mid相同 但是时间不同的数据 时间
  group by mid_id
)t4;










会员主题需求分析
===========================
===========================
需求：会员主题信息
===========================
建表语句
===========================
drop table if exists ads_user_topic;
create external table ads_user_topic(
    `dt` string COMMENT '统计日期',
    `day_users` string COMMENT '活跃会员数',
    `day_new_users` string COMMENT '新增会员数',
    `day_new_payment_users` string COMMENT '新增消费会员数',
    `payment_users` string COMMENT '总付费会员数',
    `users` string COMMENT '总会员数',
    `day_users2users` decimal(10,2) COMMENT '会员活跃率',
    `payment_users2users` decimal(10,2) COMMENT '会员付费率',
    `day_new_users2users` decimal(10,2) COMMENT '会员新鲜度'
) COMMENT '会员主题信息表'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_user_topic';
===========================
数据装载 来自于dwt_user_topic
===========================
insert into table ads_user_topic
select
'2020-03-15',
--今日活跃的数
sum(if(login_date_last='2020-03-15',1,0)),
--新增活跃的数
sum(if(login_date_first='2020-03-15',1,0)),
--新增消费会员数
sum(if(payment_date_first='2020-03-15',1,0)),
--总共付费会员数 ,只要付费了就记作你是付费会员
sum(if(payment_count>0,1,0)),
--总会员数,每个会员，加起来不就是总会员吗
sum(if(login_count>0,1,0)),
sum(if(login_date_last='2020-03-15',1,0))/sum(if(login_count>0,1,0))*100,
sum(if(payment_count>0,1,0))/sum(if(login_count>0,1,0))*100,
--会员新鲜度 今天新增的活跃会员/今天活跃的会员
sum(if(login_date_first='2020-03-15',1,0))/sum(if(login_date_last='2020-03-15',1,0))*100
from
dwt_user_topic;

===========================
建表语句  需求漏斗分析 统计“浏览->购物车->下单->支付”的转化率
===========================
drop table if exists ads_user_action_convert_day;
create external  table ads_user_action_convert_day(
    `dt` string COMMENT '统计日期',
    `total_visitor_m_count`  bigint COMMENT '总访问人数',
    `cart_u_count` bigint COMMENT '加入购物车的人数',
    `visitor2cart_convert_ratio` decimal(10,2) COMMENT '访问到加入购物车转化率',
    `order_u_count` bigint     COMMENT '下单人数',
    `cart2order_convert_ratio`  decimal(10,2) COMMENT '加入购物车到下单转化率',
    `payment_u_count` bigint     COMMENT '支付人数',
    `order2payment_convert_ratio` decimal(10,2) COMMENT '下单到支付的转化率'
 ) COMMENT '用户行为漏斗分析'
row format delimited  fields terminated by '\t'
location '/warehouse/gmall/ads/ads_user_action_convert_day/';
===========================
数据装载 来自于日活表dws_user_action_daycount
===========================
--从用户行为日表出方便
--获取一日的漏斗分析
insert into table ads_user_action_convert_day
select
'2020-03-15',
--访问人数
sum(if(login_count>0,1,0)),
--加入购物车的人数
sum(if(cart_count>0,1,0)),
--访问到加入购物车转化率
sum(if(cart_count>0,1,0))/sum(if(login_count>0,1,0))*100,
--下单人数
sum(if(order_count>0,1,0)),
--加入购物车到下单转化率
sum(if(order_count>0,1,0))/sum(if(cart_count>0,1,0))*100,
--支付人数
sum(if(payment_count>0,1,0)),
--下单到支付的转化率
sum(if(payment_count>0,1,0))/sum(if(order_count>0,1,0))
from dws_user_action_daycount
where dt='2020-03-15';







商品主题需求分析
===========================

商品个数信息  建表语句
===========================
drop table if exists ads_product_info;
create external table ads_product_info(
    `dt` string COMMENT '统计日期',
    `sku_num` string COMMENT 'sku个数',
    `spu_num` string COMMENT 'spu个数'
) COMMENT '商品个数信息'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_product_info';
===========================
数据装载 来自于dwt_uv_topic 
===========================
--多个sku可能对应多个spu 所以要单独对spu进行统计
select
'2020-03-15' dt,
count(*) sku_num
from dwt_sku_topic;

select
'2020-03-15' dt,
count(*) ct
from
( select
    spu_id
  from dwt_sku_topic
  group by spu_id
)tmp;

insert into table ads_product_info
select
'2020-03-15',
sku_num,
ct
from
(
  select
  '2020-03-15' dt,
  count(*) sku_num
  from dwt_sku_topic
)t1
join
(
  select
    '2020-03-15' dt,
    count(*) ct
  from
  ( select
      spu_id
    from dwt_sku_topic
    group by spu_id
  )tmp
)t2
on
t1.dt=t2.dt;


===========================
建表语句  销量排名需求  某一日的销量排名 ，所以取数据从日表取
===========================
drop table if exists ads_product_sale_topN;
create external table ads_product_sale_topN(
    `dt` string COMMENT '统计日期',
    `sku_id` string COMMENT '商品ID',
    `payment_amount` bigint COMMENT '销量'
) COMMENT '商品个数信息'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_product_sale_topN';
===========================
数据装载来自于 dws_sku_action_daycount
===========================
select
'2020-03-15',
sku_id,
payment_amount,
rank() over(order by order_num desc) r
from dws_sku_action_daycount
where dt = '2020-03-15';

insert into table ads_product_sale_topN
select
'2020-03-15',
sku_id,
payment_amount
from
(
  select
  '2020-03-15',
  sku_id,
  payment_amount,
  rank() over(order by order_num desc) r
  from dws_sku_action_daycount
  where dt = '2020-03-15'
)t1
where r<=10;
===========================
建表语句 收藏排名需求
===========================
drop table if exists ads_product_favor_topN;
create external table ads_product_favor_topN(
    `dt` string COMMENT '统计日期',
    `sku_id` string COMMENT '商品ID',
    `favor_count` bigint COMMENT '收藏量'
) COMMENT '商品收藏TopN'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_product_favor_topN';
===========================
数据装载来自于dws_sku_action_daycount
===========================
select
sku_id,
favor_count,
rank() over(order by favor_count desc) r
from dws_sku_action_daycount
where dt='2020-03-15';

insert into ads_product_favor_topN
select
'2020-03-15',
sku_id,
favor_count
from
(
  select
  sku_id,
  favor_count,
  rank() over(order by favor_count desc) r
  from dws_sku_action_daycount
  where dt='2020-03-15'
)t1
where r<=10;


===========================
建表语句 加入购物车主题
===========================
drop table if exists ads_product_cart_topN;
create external table ads_product_cart_topN(
    `dt` string COMMENT '统计日期',
    `sku_id` string COMMENT '商品ID',
    `cart_num` bigint COMMENT '加入购物车数量'
) COMMENT '商品加入购物车TopN'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_product_cart_topN';
===========================
数据装载来自于dws_sku_action_daycount
===========================
select
sku_id,
cart_num,
rank() over(order by cart_count desc) r
from 
dws_sku_action_daycount
where dt='2020-03-15';

insert into table ads_product_cart_topN
select
'2020-03-15',
sku_id,
cart_num
from
(
  select
    sku_id,
    cart_num,
    rank() over(order by cart_num desc) r
  from 
  dws_sku_action_daycount
  where dt='2020-03-15'
)t1
where r<=10;
===========================
商品退款率需求TOPN   退款率算三十天内的 所以取三十天的数据
===========================
drop table if exists ads_product_refund_topN;
create external table ads_product_refund_topN(
    `dt` string COMMENT '统计日期',
    `sku_id` string COMMENT '商品ID',
    `refund_ratio` decimal(10,2) COMMENT '退款率'
) COMMENT '商品退款率TopN'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_product_refund_topN';
===========================
数据装载 来自于dwt_uv_topic
===========================

select
sku_id,
refund_last_30d_count/payment_last_30d_count*100 ratio,
rank() over(order by refund_last_30d_count/payment_last_30d_count desc) r
from dwt_sku_topic;

insert into table ads_product_refund_topN
select
'2020-03-15',
sku_id,
ratio
from 
(
  select
    sku_id,
    refund_last_30d_count/payment_last_30d_count*100 ratio,
    rank() over(order by refund_last_30d_count/payment_last_30d_count desc) r
  from dwt_sku_topic
)t1
where r <= 10;

===========================
建表语句   商品差评率分析
===========================
drop table if exists ads_appraise_bad_topN;
create external table ads_appraise_bad_topN(
    `dt` string COMMENT '统计日期',
    `sku_id` string COMMENT '商品ID',
    `appraise_bad_ratio` decimal(10,2) COMMENT '差评率'
) COMMENT '商品差评率TopN'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_appraise_bad_topN';
===========================
数据装载 来自于dws_sku_action_daycount
===========================

select
sku_id,
appraise_bad_count/(appraise_good_count+appraise_mid_count+appraise_bad_count+appraise_default_count) ratio,
rank() over(order by appraise_bad_count/(appraise_good_count+appraise_mid_count+appraise_bad_count+appraise_default_count) desc) r
from
dws_sku_action_daycount
where dt = '2020-03-15';

insert into table ads_appraise_bad_topN
select
'2020-03-15',
sku_id,
ratio
from
(
  select
    sku_id,
    appraise_bad_count/(appraise_good_count+appraise_mid_count+appraise_bad_count+appraise_default_count) ratio,
    rank() over(order by appraise_bad_count/(appraise_good_count+appraise_mid_count+appraise_bad_count+appraise_default_count) desc) r
  from
  dws_sku_action_daycount
  where dt = '2020-03-15'
)t1
where r<=10;






营销主题 需求
===========================


建表语句 下单数目统计
统计每日下单数，下单金额及下单用户数
===========================
drop table if exists ads_order_daycount;
create external table ads_order_daycount(
    dt string comment '统计日期',
    order_count bigint comment '单日下单笔数',
    order_amount bigint comment '单日下单金额',
    order_users bigint comment '单日下单用户数'
) comment '每日订单总计表'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_order_daycount';
===========================
数据装载来自(dwd_fact_order_detail 获取当日下单获取数据  -- 自己写的方法)  实际上可以直接从dws_user_action_daycoun获取
===========================
select
'2020-03-15' da,
--今日下单数目
sum(if('2020-03-15'=date_format(create_time,'yyyy-MM-dd'),1,0)) count_day,
--今日下单总金额
sum(total_amount) money_day
from
dwd_fact_order_detail
where dt='2020-03-15'

select
'2020-03-15' da,
count(*) ct
from
(
  select
    count(*)
  from
  dwd_fact_order_detail
  where dt='2020-03-15'
  group by user_id
)t2


---------------------------------------自己写的方法，从dwd层获取---------------\
insert into table ads_order_daycount
select
'2020-03-15',
tmp1.count_day,
tmp1.money_day,
tmp2.ct
from
(
  select
  '2020-03-15' da,
  --今日下单数目
  sum(if('2020-03-15'=date_format(create_time,'yyyy-MM-dd'),1,0)) count_day,
  --今日下单总金额
  sum(total_amount) money_day
  from
  dwd_fact_order_detail
  where dt='2020-03-15'
)tmp1
join
(
  select
  '2020-03-15' da,
   count(*) ct
  from
  (
    select
      count(*)
    from
    dwd_fact_order_detail
    where dt='2020-03-15'
    group by user_id
  )t2
)tmp2
on tmp1.da = tmp2.da;
---------------------------------------------------------------------------
--这个有问题
insert into table ads_order_daycount
select
'2020-03-15',
sum(order_count),
sum(order_amount),
sum(if(order_count>0,1,0))
from
dws_user_action_daycount
where dt='2020-03-15';
----------------------------------------------------------------------------
===========================
建表语句  支付信息统计  
===========================
drop table if exists ads_payment_daycount;
create external table ads_payment_daycount(
    dt string comment '统计日期',
    payment_count bigint comment '单日支付笔数',
    payment_amount bigint comment '单日支付金额',
    payment_user_count bigint comment '单日支付人数',
    payment_sku_count bigint comment '单日支付商品数',
    payment_avg_time double comment '下单到支付的平均时长，取分钟数'
) comment '每日订单总计表'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_payment_daycount';
===========================
数据装载 来自于dwd_fact_order_info dws_user_action_daycount dws_sku_action_daycount
===========================
-----------------------------------------------------------------------------
--计算出单日支付笔数，单日支付金额
select
'2020-03-15' da, 
sum(if('2020-03-15'=date_format(payment_time,'yyyy-MM-dd'),1,0)) pay_count,
sum(payment_amount) pay_moeny
from dwd_fact_payment_info
where dt='2020-03-15';

--单日支付人数
select
'2020-03-15' da,
count(*) ct
from
(
  select
    count(*)
  from
  dwd_fact_payment_info
  where dt='2020-03-15'
  group by user_id
)t1;

select
'2020-03-15' da,
sum(sku_num) sku_num
from dwd_fact_order_detail
where dt='2020-03-15';


select
'2020-03-15',
tmp1.pay_count,
tmp1.pay_moeny,
tmp2.ct,
tmp3.sku_num
from
(
  select
    '2020-03-15' da, 
    sum(if('2020-03-15'=date_format(payment_time,'yyyy-MM-dd'),1,0)) pay_count,
    sum(payment_amount) pay_moeny
  from dwd_fact_payment_info
  where dt='2020-03-15'
)tmp1
join
(
  select
    '2020-03-15' da,
    count(*) ct
  from
  (
    select
      count(*)
    from
    dwd_fact_payment_info
    where dt='2020-03-15'
    group by user_id
)t1
)tmp2
on tmp1.da = tmp2.da
join
(
  select
    '2020-03-15' da,
    sum(sku_num) sku_num
  from dwd_fact_order_detail
  where dt='2020-03-15'
)tmp3
on tmp1.da = tmp3.da;
----------------------------------------------------




insert into table ads_payment_daycount
select
    tmp_payment.dt,
    tmp_payment.payment_count,
    tmp_payment.payment_amount,
    tmp_payment.payment_user_count,
    tmp_skucount.payment_sku_count,
    tmp_time.payment_avg_time
from
(
    select
        '2020-03-15' dt,
        sum(payment_count) payment_count,
        sum(payment_amount) payment_amount,
        sum(if(payment_count>0,1,0)) payment_user_count
    from dws_user_action_daycount
    where dt='2020-03-15'
)tmp_payment
join
(
    select
        '2020-03-15' dt,
        sum(if(payment_count>0,1,0)) payment_sku_count 
    from dws_sku_action_daycount
    where dt='2020-03-15'
)tmp_skucount on tmp_payment.dt=tmp_skucount.dt
join
(
    select
        '2020-03-15' dt,
        sum(unix_timestamp(payment_time)-unix_timestamp(create_time))/count(*)/60 payment_avg_time
    from dwd_fact_order_info
    where dt='2020-03-15'
    and payment_time is not null
)tmp_time on tmp_payment.dt=tmp_time.dt;

===========================
复购率




最后返回结果要求

服装   李宁   ---------
服装   nike   -------------
服装   安踏    --------------
===========================
drop table ads_sale_tm_category1_stat_mn;
create external table ads_sale_tm_category1_stat_mn
(  
    tm_id string comment '品牌id',
    category1_id string comment '1级品类id ',
    category1_name string comment '1级品类名称 ',
    buycount   bigint comment  '购买人数',
    buy_twice_last bigint  comment '两次以上购买人数',
    buy_twice_last_ratio decimal(10,2)  comment  '单次复购率',
    buy_3times_last   bigint comment   '三次以上购买人数',
    buy_3times_last_ratio decimal(10,2)  comment  '多次复购率',
    stat_mn string comment '统计月份',
    stat_date string comment '统计日期' 
)   COMMENT '复购率统计'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_sale_tm_category1_stat_mn/';
===========================
数据装载
===========================
select
t1.sku_tm_id,
t1.sku_category1_id,
t1.sku_category1_name,
t2.pay1,
t2.pay2,
t2.pay3
from
(select
'2020-03-15' dt,
sku_tm_id,
sku_category1_id,
sku_category1_name
from
dws_sale_detail_daycount
where dt='2020-03-15')t1
join
(select
 '2020-03-15' dt, 
sum(if(sku_num>0,1,0)) pay1,
sum(if(sku_num>=2,1,0))pay2,
sum(if(sku_num>=3,1,0))pay3
from
dws_sale_detail_daycount
where dt='2020-03-15')t2
on t1.dt = t2.dt;



select
    distinct(tm_name),
    t3.sku_tm_id,
    t3.sku_category1_id,
    t3.sku_category1_name,
    t3.pay1,
    t3.pay2,
    t3.pay2/t3.pay1*100,
    t3.pay3,
    t3.pay3/t3.pay1*100,
    date_format('2020-03-15','yyyy-MM'),
    '2020-03-15'
from
(
  select
    t1.sku_tm_id,
    t1.sku_category1_id,
    t1.sku_category1_name,
    t2.pay1,
    t2.pay2,
    t2.pay3
  from
  (select
  '2020-03-15' dt,
  sku_tm_id,
  sku_category1_id,
  sku_category1_name
  from
  dws_sale_detail_daycount
  where dt='2020-03-15')t1
  join
  (select
   '2020-03-15' dt, 
  sum(if(sku_num>0,1,0)) pay1,
  sum(if(sku_num>=2,1,0))pay2,
  sum(if(sku_num>=3,1,0))pay3
  from
  dws_sale_detail_daycount
  where dt='2020-03-15')t2
  on t1.dt = t2.dt
)t3
left join ods_base_trademark
on t3.sku_tm_id = tm_id;





===========================
建表语句
===========================
===========================
数据装载
===========================

===========================
建表语句
===========================
===========================
数据装载
===========================

===========================
  
建表语句
===========================
===========================
数据装载
===========================

===========================
建表语句
===========================
===========================
数据装载
===========================

===========================
建表语句
===========================
===========================
数据装载
===========================

===========================
建表语句
===========================
===========================
数据装载
===========================

===========================
建表语句
===========================
===========================
数据装载
===========================

===========================
建表语句
===========================
===========================
数据装载
===========================

===========================
  
建表语句
===========================
===========================
数据装载
===========================

===========================
建表语句
===========================
===========================
数据装载
===========================

===========================
建表语句
===========================
===========================
数据装载
===========================

===========================
建表语句
===========================
===========================
数据装载
===========================

===========================


















