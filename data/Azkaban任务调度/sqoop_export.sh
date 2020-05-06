#!/bin/bash

hive_db_name=gmall
mysql_db_name=gmall_report

export_data() {
/opt/module/sqoop/bin/sqoop export \
--connect "jdbc:mysql://hadoop105:3306/${mysql_db_name}?useUnicode=true&characterEncoding=utf-8"  \
--username root \
--password 123456 \
--table $1 \
--num-mappers 1 \
--export-dir /warehouse/$hive_db_name/ads/$1 \
--input-fields-terminated-by "\t" \
--update-mode allowinsert \
--update-key $2 \
--input-null-string '\\N'    \
--input-null-non-string '\\N'
}

case $1 in
  "ads_uv_count")
     export_data "ads_uv_count" "dt"
;;
  "ads_user_action_convert_day") 
     export_data "ads_user_action_convert_day" "dt"
;;
  "ads_user_topic")
     export_data "ads_user_topic" "dt"
;;
   "all")
     export_data "ads_appraise_bad_topN" "dt"
     export_data "ads_back_count" "dt"
     export_data "ads_continuity_uv_count" "dt"
     export_data "ads_continuity_wk_count" "dt"
     export_data "ads_new_mid_count" "dt"
     export_data "ads_order_daycount" "dt"
     export_data "ads_payment_daycount" "dt"
     export_data "ads_product_cart_topN" "dt"
     export_data "ads_product_favor_topN" "dt"
     export_data "ads_product_info" "dt"
     export_data "ads_product_refund_topN" "dt"
     export_data "ads_product_sale_topN" "dt"
     export_data "ads_sale_tm_category1_stat_mn" "dt"
     export_data "ads_silent_count" "dt"
     export_data "ads_user_action_convert_day" "dt"
     export_data "ads_user_retention_day_rate" "dt"
     export_data "ads_user_topic" "dt"
     export_data "ads_uv_count" "dt"
     export_data "ads_wastage_count" "dt"
;;
esac