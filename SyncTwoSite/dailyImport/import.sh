#!/bin/bash

SERVER_VERSION="0.0.4"

#dateString=`date --date='yesterday'`
dateString=$1

SCRIPT_DIR=$(dirname $0)

REMOTE_IP="221.4.141.146"
REMOTE_PORT="2200"
REMOTE_SCRIPT_DIR="/home/zhenhai/dailyDump"
REMOTE_SCRIPT_DUMP="dump.sh"
REMOTE_SCRIPT_CLEAN="clean.sh"

echo "============= DUMP REMOTE DATABASE ============="

ssh -p $REMOTE_PORT zhenhai@$REMOTE_IP $REMOTE_SCRIPT_DIR/$REMOTE_SCRIPT_DUMP $dateString

echo "============= RESTORE DATABASE ============="

scp -P $REMOTE_PORT zhenhai@$REMOTE_IP:$REMOTE_SCRIPT_DIR/$dateString.tar.gz $dateString.tar.gz
#
tar -xvzf $dateString.tar.gz
mongorestore --db zhenhai --drop export$dateString/zhenhai

rm -rvf $dateString.tar.gz
rm -rvf export$dateString/
