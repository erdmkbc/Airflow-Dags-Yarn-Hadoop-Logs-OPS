#!/bin/bash

now=$(date +%s)
hadoop fs -ls -R | while read f; do
 dir_date=`echo $f | awk '{print $6}'`
 difference=$(( ( $now - $(date -d "$dir_date" +%s) ) / (24 * 60 * 60 ) ))
 hive -e "insert into table logs_resources.hdfs_file_lifecycle values('${f}', '${difference}');"
 if [ $difference -gt 10 ]; then
   echo $f; echo $difference;
 fi
done

