FROM openwhisk/alarmprovider:8b30d12

RUN apt-get update && apt-get upgrade -y

COPY package.json /alarmsTrigger/
RUN cd /alarmsTrigger && npm install --production

COPY authHandler.js /alarmsTrigger/lib/
