#!/bin/bash
dbname=large2
ntotal=100

tstart=`date +%s`
echo "Adding $dbname" >> ../logs/loadMongoTime.log

for (( incr=0; incr<$ntotal; incr++ ))
do
	t0=`date +%s`
	python write_to_mongo.py $dbname $incr
	t1=`date +%s`
	runtime=$((t1-t0))
	echo "$dbname $incr, $runtime" >> ../logs/loadMongoTime.log
done

./addStations.sh $dbname

tend=`date +%s`
ttotal=$((tstart-tend))
echo "$dbname took $ttotal s" >> ../logs/loadMongoTime.log
