#!/bin/bash

#dateString=`date --date='yesterday'`
#dateString="2014-10-09"
dateString=$1

######## OUTPUT SETTING ##################
exportDir="export$dateString"
exportFile="$dateString.tar.gz"

######## START ####################

cd "$(dirname "$0")"
echo "Dump mongoDB collection $dateString..."
mongodump --db zhenhai -o $exportDir

tar -cvzf $exportFile $exportDir/

