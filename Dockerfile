FROM node:10.20.1

RUN apt-get update && apt-get upgrade -y

ADD package.json /alarmsTrigger/
RUN cd /alarmsTrigger && npm install --production

ADD provider/. /alarmsTrigger/
