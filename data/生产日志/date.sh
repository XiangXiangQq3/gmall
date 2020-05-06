#! /bin/bash
#同步日期脚本
for i in hadoop102 hadoop103 hadoop104; do
    echo "========== $i =========="
    ssh $i "sudo date -s $1"
done
