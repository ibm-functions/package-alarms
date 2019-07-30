FROM openwhisk/alarmprovider:0a24b1b

RUN apt-get update && apt-get upgrade -y

COPY package.json /alarmsTrigger/
RUN cd /alarmsTrigger && npm install --production

COPY authHandler.js /alarmsTrigger/lib/
