FROM openwhisk/alarmprovider:1.12.2

COPY package.json /alarmsTrigger/
RUN cd /alarmsTrigger && npm install --production

COPY authHandler.js /alarmsTrigger/lib/
