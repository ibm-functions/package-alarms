FROM node:20

RUN apt-get update && apt-get upgrade -y

# Remove CURL and git as it is has constant security vulnerabilities and we don't use it
RUN apt-get purge -y --auto-remove curl git

ADD package.json /alarmsTrigger/
# let npm install call Fail in case of not matching npm engine requirements .
## Because of NODE:20 not listed for ibmcloud-cloudant do not use strict##   RUN echo "engine-strict=true" > /alarmsTrigger/.npmrc
RUN echo "engine-strict=false" > /alarmsTrigger/.npmrc
RUN cd /alarmsTrigger && npm install --omit=dev

ADD provider/. /alarmsTrigger/
