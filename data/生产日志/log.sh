#生产日志
#! /bin/bash
for i in hadoop102 hadoop103; do
    echo "========== $i =========="
    ssh $i "java -jar /home/atguigu/lib/user_log_collector-1.0-SNAPSHOT-jar-with-dependencies.jar $1 $2 >/dev/null 2>&1"
done
