#! /bin/bash
case $1 in
    start )
        for i in hadoop104; do
            echo "========== $i =========="
            ssh $i "nohup /opt/module/flume-1.7.0/bin/flume-ng agent -n a1 -c /opt/module/flume-1.7.0/conf/ -f /opt/module/flume-1.7.0/jobs/kafka_hdfs.conf >/dev/null 2>&1 &"
        done
    ;;
    stop )
        for i in hadoop104; do
            echo "========== $i =========="
            ssh $i "ps -ef | awk '/kafka_hdfs.conf/ && !/awk/{print \$2}' | xargs kill -9"
        done
    ;;
esac
