#!/bin/bash
#将hdfs上的各个话题的数据装载到ods层
db=gmall
hive=/opt/module/hive/bin/hive
do_date=`date -d '-1 day' +%F`

if [[ -n "$1" ]]; then
    do_date=$1
fi

sql="
load data inpath '/origin_data/gmall/log/topic_start/$do_date' into table ${db}.ods_start_log partition(dt='$do_date');
load data inpath '/origin_data/gmall/log/topic_event/$do_date' into table ${db}.ods_event_log partition(dt='$do_date');
"
#建索引index
$hive -e "$sql"
hadoop jar /opt/module/hadoop-2.7.2/share/hadoop/common/hadoop-lzo-0.4.20.jar com.hadoop.compression.lzo.DistributedLzoIndexer /warehouse/gmall/ods/ods_start_log/dt=$do_date
hadoop jar /opt/module/hadoop-2.7.2/share/hadoop/common/hadoop-lzo-0.4.20.jar com.hadoop.compression.lzo.DistributedLzoIndexer /warehouse/gmall/ods/ods_event_log/dt=$do_date