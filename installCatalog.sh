#!/bin/bash
set -ex

if [ ! -d ./openwhisk-alarms ]; then
 git clone https://github.com/apache/incubator-openwhisk-package-alarms openwhisk-alarms
fi

cp config.js ./openwhisk-alarms/action/lib/config.js

cd openwhisk-alarms
./installCatalog.sh $1 $2 $3 $4 $5 $6