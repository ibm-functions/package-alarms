#!/bin/bash

SCRIPTDIR=$(cd $(dirname "$0") && pwd)
ROOTDIR="$SCRIPTDIR/../.."

cd $ROOTDIR
git clone https://github.com/apache/incubator-openwhisk-package-alarms openwhisk-alarms
