FROM openwhisk/alarmprovider:1.11.0

COPY package.json /alarmsTrigger/
RUN cd /alarmsTrigger && npm install --production

COPY authHandler.js /alarmsTrigger/lib/
