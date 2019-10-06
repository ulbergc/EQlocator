# Used to augment dataset with new event ids and randomized observations
# 1. Make a folder for a month - it will have ncopy days added to it
# 2. call update_json ncopy times, sending output to the new folder
# 3. once finished, concatenate all files together into one large .json file
# 4. delete the small files
# 5. move on to the next month
# Separately, upload files to S3

homedir=/data
# bigdir is where the new files will be writte
bigdir=/data/BIG_JSON

ncopy=10

mn0=0
mn1=12

DATE0=19800101

tstart=`date +%s`

# first loop over months
for (( i_m=$mn0; i_m<$mn1; i_m++ ))
do
	cd $homedir
	MN_DATE=$(date +%Y%m%d -d "$DATE0 + $i_m month")
	t0=`date +%s`
	total_time=$(( t0-tstart ))
	msg=$(printf "Doing %s, %.1f s" "$MN_DATE" "$total_time")
	echo "$msg"
	
	# make folder
	dir_name=`echo "$bigdir/raw/$MN_DATE"`
	mkdir -p $dir_name

	# add file to folder
	for (( i_d=0; i_d<$ncopy; i_d++ ))
	do
		NEXT_DATE=$(date +%Y%m%d -d "$MN_DATE + $i_d day")
		python augment_json.py $dir_name $NEXT_DATE
		t_tmp=`date +%s`
		running_time=$(( t_tmp-t0 ))
		msg=$(printf "%s finished, %.1f s" "$NEXT_DATE" "$running_time")
		echo "    $msg"
	done
	
	# concatenate files
	cd $dir_name
	mkdir -p $bigdir/big/$MN_DATE
	echo "    Moving to $bigdir/big/$MN_DATE/$MN_DATE.json"
	find . -type f -exec awk '{print $0}' {} + > $bigdir/big/$MN_DATE/$MN_DATE.json

	# delete old files
	cd $homedir
	rm -r $dir_name
	t_tmp=`date +%s`
        running_time=$(( t_tmp-t0 ))
	msg=$(printf "%s took %.1f s" "$MN_DATE" "$running_time")
	echo "    $msg"
done
