# Input values
prog_name=process_data_s3.py
read_db_name=tmp4
read_table_name=c0
py_args=$( echo "$read_db_name $read_table_name" )

starttime=`date +%s`
PACKAGE=$(echo "--packages com.databricks:spark-csv_2.10:1.2.0,mysql:mysql-connector-java:8.0.17")

masterDNS=10.0.0.13
MASTER=$(echo "--master spark://$masterDNS:7077")
#MASTER=
echo $MASTER

echo "Calling spark-submit $PACKAGE $MASTER $prog_name $py_args"

spark-submit $PACKAGE $MASTER $prog_name $py_args

endtimeFull=`date`
endtime=`date +%s`
runtime=$((endtime-starttime))
echo "Testing"
echo "$endtimeFull | $runtime | spark-submit $PACKAGE $MASTER $prog_name $py_args" >> sparktime.log
