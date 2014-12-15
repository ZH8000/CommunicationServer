#!/bin/bash

SERVER_VERSION="0.0.3"

#dateString=`date --date='yesterday'`
dateString=$1

SCRIPT_DIR=$(dirname $0)

REMOTE_IP="221.4.141.146"
REMOTE_PORT="2200"
REMOTE_SCRIPT_DIR="/home/zhenhai/dailyDump"
REMOTE_SCRIPT_DUMP="dump.sh"
REMOTE_SCRIPT_CLEAN="clean.sh"

ssh -p $REMOTE_PORT zhenhai@$REMOTE_IP $REMOTE_SCRIPT_DIR/$REMOTE_SCRIPT_DUMP $dateString
scp -P $REMOTE_PORT zhenhai@$REMOTE_IP:$REMOTE_SCRIPT_DIR/$dateString.tar.gz $SCRIPT_DIR/$dateString.tar.gz

tar -xvzf $SCRIPT_DIR/$dateString.tar.gz
mongorestore --db zhenhaiDaily --drop export$dateString/zhenhaiDaily
rm -rvf $dateString.tar.gz
rm -rvf export$dateString/
# 
java -cp $SCRIPT_DIR/CommunicationServer-assembly-$SERVER_VERSION.jar tw.com.zhenhai.main.DailyImport $dateString
# 
ssh -p $REMOTE_PORT zhenhai@$REMOTE_IP $REMOTE_SCRIPT_DIR/$REMOTE_SCRIPT_CLEAN

