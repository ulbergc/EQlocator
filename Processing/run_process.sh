#!/bin/bash
# Spark input values
num_executors=6
executor_cores=6
executor_memory=4

# Input values
prog_name=process_data.py
write_db_name=$1
write_table_name=$2
method=$3
py_args=$( echo "$write_db_name $write_table_name $method" )

EXTRA_ARGS=$(echo "--conf \"spark.dynamicAllocation.enable=true\" --conf \"spark.dynamicAllocation.executorIdleTimeout=2m\" --conf \"spark.dynamicAllocation.minExecutors=1\" --conf \"spark.dynamicAllocation.maxExecutors=2000\" --conf \"spark.stage.maxConsecutiveAttempts=10\" --conf \"spark.memory.offHeap.enable=true\" --conf \"spark.memory.offHeap.size=3g\" --conf \"spark.yarn.executor.memoryOverhead=0.1*(spark.executor.memory+spark.memory.offHeap.size)\"")
#echo $EXTRA_ARGS
#exit
EXTRA_ARGS=

starttime=`date +%s`
SPARK_ARGS=$(echo "--num-executors ${num_executors} --executor-cores ${executor_cores} --executor-memory ${executor_memory}G --driver-memory 4G --conf "spark.driver.cores=4"")
#SPARK_ARGS=$(echo "--executor-memory 5G")
# could add driver-memory
PACKAGE=$(echo "--packages com.databricks:spark-csv_2.10:1.2.0,mysql:mysql-connector-java:8.0.17")

masterDNS=10.0.0.6
MASTER=$(echo "--master spark://$masterDNS:7077")
#MASTER=
echo $MASTER

echo "Calling spark-submit $PACKAGE $MASTER $SPARK_ARGS $prog_name $py_args"

spark-submit $PACKAGE $MASTER $SPARK_ARGS $EXTRA_ARGS $prog_name $py_args

endtimeFull=`date`
endtime=`date +%s`
runtime=$((endtime-starttime))
echo "Finished"
echo "$endtimeFull | $runtime | spark-submit $PACKAGE $MASTER $SPARK_ARGS $prog_name $py_args" >> ../logs/sparktime.log
