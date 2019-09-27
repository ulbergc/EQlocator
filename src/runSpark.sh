
narg=$#
if [ $narg -lt 1 ]; then
  echo "Usage: ./runSpark.sh <python_script>"
  exit
fi

read_db_name=tmp4
read_table_name=c0


starttime=`date +%s`
PACKAGE=$(echo "--packages org.mongodb.spark:mongo-spark-connector_2.11:2.4.1,mysql:mysql-connector-java:8.0.17")

masterDNS=10.0.0.13
MASTER=$(echo "--master spark://$masterDNS:7077")
#MASTER=
echo $MASTER

echo "Calling spark-submit $PACKAGE $MASTER $1 $read_db_name $read_table_name"

spark-submit $PACKAGE $MASTER $1 $read_db_name $read_table_name

endtimeFull=`date`
endtime=`date +%s`
runtime=$((endtime-starttime))
echo "Testing"
echo "$read_db_name | $read_table_name | $endtimeFull | $runtime | spark-submit $PACKAGE $MASTER $1" >> ../logs/sparktime.log
