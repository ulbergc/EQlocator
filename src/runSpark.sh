
narg=$#
if [ $narg -lt 1 ]; then
  echo "Usage: ./runSpark.sh <python_script>"
  exit
fi

starttime=`date +%s`
PACKAGE=$(echo "--packages org.mongodb.spark:mongo-spark-connector_2.11:2.4.1,mysql:mysql-connector-java:8.0.17")

masterDNS=10.0.0.13
MASTER=$(echo "--master spark://$masterDNS:7077")
#MASTER=
echo $MASTER

echo "Calling spark-submit $PACKAGE $MASTER $1"

spark-submit $PACKAGE $MASTER $1

endtimeFull=`date`
endtime=`date +%s`
runtime=$((endtime-starttime))
echo "Testing"
echo "$endtimeFull | $runtime | spark-submit $PACKAGE $MASTER $1" >> ../logs/sparktime.log
