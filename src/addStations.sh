
narg=$#
if [ $narg -lt 1 ]; then
	echo "Usage: ./addStations.sh <dbname>"
	exit
fi

mongoimport -d $1 -c sta --type csv --file ../seed/stations.csv --fields Station,Longitude,Latitude,Depth
