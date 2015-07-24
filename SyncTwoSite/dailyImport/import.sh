#!/bin/bash

SERVER_VERSION="0.0.4"

dateString=`date +%Y-%m-%d`
yDateString=`date +%Y-%m-%d --date='yesterday'`

#dateString=$1

SCRIPT_DIR=$(dirname $0)

REMOTE_IP="221.4.141.146"
REMOTE_PORT="2200"
REMOTE_SCRIPT_DIR="/home/zhenhai/dailyDump"
REMOTE_SCRIPT_DUMP="dump.sh"
REMOTE_SCRIPT_CLEAN="clean.sh"

echo "============= DUMP REMOTE DATABASE ============="

ssh -p $REMOTE_PORT zhenhai@$REMOTE_IP $REMOTE_SCRIPT_DIR/$REMOTE_SCRIPT_DUMP $dateString

echo "============= RESTORE DATABASE ============="

scp -P $REMOTE_PORT zhenhai@$REMOTE_IP:$REMOTE_SCRIPT_DIR/daily$yDateString.tar.gz backup/raw/$dateString.tar.gz
scp -P $REMOTE_PORT zhenhai@$REMOTE_IP:$REMOTE_SCRIPT_DIR/$dateString.tar.gz $dateString.tar.gz

#
tar -xvzf $dateString.tar.gz
mongorestore --db zhenhai --drop export$dateString/zhenhai

rm -rvf export$dateString/
mv $dateString.tar.gz backup/

ssh -p $REMOTE_PORT zhenhai@$REMOTE_IP $REMOTE_SCRIPT_DIR/$REMOTE_SCRIPT_CLEAN

