do_date=`date -d '-1 day' +%F`

bin/sqoop import \
--connect jdbc:mysql://hadoop105:3306/gmall \
--username root \
--password 123456 \
--query "select id,name from user_info where create_time='$do_date' and \$CONDITIONS" \
--target-dir /origin_data/gmall/db/user_info/$do_date \
--delete-target-dir \
--fields-terminated-by '\t' \
--num-mappers 2 \
--split-by id



##date -d '-1 day' +%Y-%m-%d  = date -d '-1 day' +%F


-Dmaven.wagon.http.ssl.insecure=true -Dmaven.wagon.http.ssl.allowall=true -Dmaven.wagon.http.ssl.ignore.validity.dates=true