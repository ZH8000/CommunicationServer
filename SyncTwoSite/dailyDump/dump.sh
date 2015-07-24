#!/bin/bash

#dateString=`date --date='yesterday'`
#dateString="2014-10-09"
dateString=$1
yDateString=`date --date='yesterday' +%Y-%m-%d`


######## OUTPUT SETTING ##################
exportDir="export$dateString"
exportFile="$dateString.tar.gz"
exportDailyDir="daily$yDateString"
exportDailyFile="daily$yDateString.tar.gz"

######## START ####################

cd "$(dirname "$0")"
echo "Dump mongoDB collection $dateString..."
mongodump --db zhenhai -o $exportDir
mongodump --db zhenhaiRaw --collection $dateString -o $exportDailyDir

tar -cvzf $exportFile $exportDir/
tar -cvzf $exportDailyFile $exportDailyDir/

