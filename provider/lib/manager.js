/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

var needle = require('needle');
var HttpStatus = require('http-status-codes');
var lt = require('long-timeout');
var constants = require('./constants.js');
var DateAlarm = require('./dateAlarm.js');
var CronAlarm = require('./cronAlarm.js');
var IntervalAlarm = require('./intervalAlarm.js');
var Sanitizer = require('./sanitizer');
var authHandler = require('./authHandler');

module.exports = function (logger, triggerDB, redisClient, databaseName) {

    var redisKeyPrefix = process.env.REDIS_KEY_PREFIX || triggerDB.config.db;
    var self = this;

    self.triggers = {};
    self.endpointAuth = process.env.ENDPOINT_AUTH;
    self.routerHost = process.env.ROUTER_HOST || 'localhost';
    self.worker = process.env.WORKER || 'worker0';
    self.host = process.env.HOST_INDEX || 'host0';
    self.hostPrefix = this.host.replace(/\d+$/, '');
    self.activeHost = `${this.hostPrefix}0`; //default value on init (will be updated for existing redis)
    self.triggerDB = triggerDB;
    self.redisClient = redisClient;
    self.redisKey = redisKeyPrefix + '_' + this.worker;
    self.redisField = constants.REDIS_FIELD;
    self.uriHost = 'https://' + this.routerHost;
    self.sanitizer = new Sanitizer(logger, this);
    self.monitoringAuth = process.env.MONITORING_AUTH;
    self.monitorStatus = {};
    self.retrying = {};
    self.databaseName = databaseName;


    function createTrigger(triggerIdentifier, newTrigger) {
        var method = 'createTrigger';

        var callback = function onTick() {
            var triggerHandle = self.triggers[triggerIdentifier];
            if (triggerHandle && shouldFireTrigger(triggerHandle) && hasTriggersRemaining(triggerHandle)) {
                try {
                    fireTrigger(triggerHandle);
                } catch (e) {
                    logger.error(method, triggerIdentifier, ': Exception occurred while firing trigger :', e);
                }
            }
        };

        newTrigger.uri = self.uriHost + '/api/v1/namespaces/' + newTrigger.namespace + '/triggers/' + newTrigger.name;
        newTrigger.triggerID = triggerIdentifier;
        if (newTrigger.monitor) {
            newTrigger.apikey = self.monitoringAuth;
        }

        var alarm;
        if (newTrigger.date) {
            alarm = new DateAlarm(logger, newTrigger);
        } else if (newTrigger.minutes) {
            alarm = new IntervalAlarm(logger, newTrigger);
        } else {
            alarm = new CronAlarm(logger, newTrigger);
        }

        return alarm.scheduleAlarm(triggerIdentifier, callback);
    }


    function disableTrigger(triggerIdentifier,  statusCode, message) {
        var method = 'disableTrigger';

        self.triggerDB.getDocument({
            db: self.databaseName,
            docId: triggerIdentifier
        })
        .then(response => {
            var existingConfig = response.result;
            if (!existingConfig.status || existingConfig.status.active === true) {
                var updatedTriggerConfig = existingConfig;
                updatedTriggerConfig.status = {
                    'active': false,
                    'dateChanged': Date.now(),
                    'reason': {'kind': 'AUTO', 'statusCode': statusCode, 'message': message}
                };
                self.triggerDB.putDocument({
                    db: self.databaseName,
                    docId: triggerIdentifier,
                    document: updatedTriggerConfig
                }).then(response => {
                    logger.info(method, triggerIdentifier, ': Trigger successfully disabled in database');
                })
                .catch( (err) => {
                    logger.error(method, triggerIdentifier, ': There was an error while disabling in database :', err);
                })
            }

        })
        .catch( (err) => {
            logger.info(method,triggerIdentifier, ': Could not find trigger in database : '+ err);
            //make sure it is already stopped
            stopTrigger(triggerIdentifier);
                      
        })
   }

    function stopTrigger(triggerIdentifier) {
        var method = 'stopTrigger';

        if (self.triggers[triggerIdentifier]) {
            if (self.triggers[triggerIdentifier].cronHandle) {
                self.triggers[triggerIdentifier].cronHandle.stop();
            } else if (self.triggers[triggerIdentifier].intervalHandle) {
                lt.clearInterval(self.triggers[triggerIdentifier].intervalHandle);
            }
            delete self.triggers[triggerIdentifier];
            logger.info(method, triggerIdentifier, ': Trigger successfully deleted from memory');
        }
    }

    function fireTrigger(triggerData) {
        var method = 'fireTrigger';

        var triggerIdentifier = triggerData.triggerID;

        logger.info(method, triggerIdentifier, ': Alarm trigger ready to fire' );
        postTrigger(triggerData, 0)
        .then(triggerId => {
            logger.info(method, triggerId,  ': Trigger was successfully fired');
            handleFiredTrigger(triggerData);
        })
        .catch(err => {
            logger.error(method,  triggerIdentifier, ": Failed posting a trigger :" + err);
            handleFiredTrigger(triggerData);
        });
    }

    function postTrigger(triggerData, retryCount, throttleCount) {
        var method = 'postTrigger';
        var isIAMNamespace = triggerData.additionalData && triggerData.additionalData.iamApikey;
        var triggerIdentifier = triggerData.triggerID;

        if (retryCount > 0 && !self.retrying[triggerIdentifier]) {
            // this is a retry of a previously failed trigger which was
            // successfully triggered in the meantime, thus we should abort.
            return Promise.reject(`Aborting retry ${retryCount} for trigger post, has been fired successfully`);
        }

        return new Promise(function (resolve, reject) {

            // only manage trigger fires if they are not infinite
            if (triggerData.maxTriggers && triggerData.maxTriggers !== -1) {
                triggerData.triggersLeft--;
            }

            self.authRequest(triggerData, {
                method: 'post',
                uri: triggerData.uri
            },triggerData.payload ,
            function (error, response, source ) {
                try {
                    var statusCode = response ? response.statusCode : undefined;
                    var headers = response ? response.headers : undefined;
                    
                    if (error && source == "auth_handling") {
                    	logger.error(method, triggerIdentifier, ': Error in handleAuth() request for trigger : ', error);
                    }
                    
                    //check for IAM auth error and ignore for now (do not disable) due to bug with IAM
                    if (source == "auth_handling" && error && error.statusCode === 400) {
                        var message;
                        try {
                            message = `${error.error.errorMessage} in generating IAM token for ${triggerIdentifier}, requestId: ${error.error.context.requestId}`;
                        } catch (e) {
                            message = `Received an error generating IAM token for ${triggerIdentifier}: ${error}`;
                        }
                        reject(message);
                    } else if (error || statusCode >= 400) {
                        logger.error(method, triggerIdentifier, ': Received an error invoking :',  statusCode || error);
                        var throttleCounter = throttleCount || 0;

                        // only manage trigger fires if they are not infinite
                        if (triggerData.maxTriggers && triggerData.maxTriggers !== -1) {
                            triggerData.triggersLeft++;
                        } else if (statusCode === HttpStatus.NOT_FOUND && hasTransactionIdHeader(headers)) {
                            self.sanitizer.deleteTriggerFeed(triggerIdentifier);
                            reject(`Deleted trigger feed ${triggerIdentifier}: Received a 404 when firing the trigger`);
                        } else if (statusCode && shouldDisableTrigger(statusCode, headers, isIAMNamespace)) {
                            var errMsg = `Received a ${statusCode} status code when firing the trigger`;
                            disableTrigger(triggerIdentifier, statusCode, `Trigger automatically disabled: ${errMsg}`);
                            reject(`Disabled trigger ${triggerIdentifier}: ${errMsg}`);
                        } else {
                            // only start a retry loop once (when self.retrying is unset).
                            // retryCount > 0 means this already is a retry so we can continue
                            if (retryCount < constants.RETRY_ATTEMPTS && (!self.retrying[triggerIdentifier] || retryCount > 0)) {
                                if (retryCount === 0) {
                                    self.retrying[triggerIdentifier] = true;
                                }
                                throttleCounter = statusCode === HttpStatus.TOO_MANY_REQUESTS ? throttleCounter + 1 : throttleCounter;
                                const retryDelay = Math.max(constants.RETRY_DELAY, 1000 * Math.pow(throttleCounter, 2));
                                logger.info(method,  triggerIdentifier, ': Attempt to fire trigger again in ', retryDelay, 'ms, retry count:', (retryCount + 1));
                                setTimeout(function () {
                                    postTrigger(triggerData, (retryCount + 1), throttleCounter)
                                    .then(triggerId => {
                                        resolve(triggerId);
                                    })
                                    .catch(err => {
                                        reject(err);
                                    });
                                }, retryDelay);
                            } else {
                                if (throttleCounter === constants.RETRY_ATTEMPTS) {
                                    var msg = 'Automatically disabled after continuously receiving a 429 status code when firing the trigger';
                                    disableTrigger(triggerIdentifier, 429, msg);
                                    reject('Disabled trigger ' + triggerIdentifier + ' due to status code: 429');
                                } else if (retryCount === 0 && self.retrying[triggerIdentifier]) {
                                    reject('Unable to reach server to fire trigger ' + triggerIdentifier + ', another retry currently in progress');
                                } else {
                                    reject('Unable to reach server to fire trigger ' + triggerIdentifier);
                                    self.retrying[triggerIdentifier] = false;
                                }
                            }
                        }
                    } else {
                        self.retrying[triggerIdentifier] = false;
                        logger.info(method, triggerIdentifier, ': Fired trigger request,', 'Status Code:', statusCode);
                        resolve(triggerIdentifier);
                    }
                } catch (err) {
                	self.retrying[triggerIdentifier] = false;
                    reject('Exception occurred while firing trigger : ' + err);
                }
            });
        });
    }

    function shouldDisableTrigger(statusCode, headers, isIAMNamespace) {
        //temporary workaround for IAM issues
        // do not disable for 401s or 403s for IAM namespaces
        if ((statusCode === HttpStatus.FORBIDDEN || statusCode === HttpStatus.UNAUTHORIZED) && isIAMNamespace) {
            return false;
        }

        return statusCode === HttpStatus.BAD_REQUEST || ((statusCode > 400 && statusCode < 500) && hasTransactionIdHeader(headers) &&
            [HttpStatus.REQUEST_TIMEOUT, HttpStatus.TOO_MANY_REQUESTS, HttpStatus.CONFLICT].indexOf(statusCode) === -1);
    }

    function hasTransactionIdHeader(headers) {
        return headers && headers['x-request-id'];
    }

    function shouldFireTrigger(trigger) {
        return trigger.monitor || self.activeHost === self.host;
    }

    function hasTriggersRemaining(trigger) {
        return !trigger.maxTriggers || trigger.maxTriggers === -1 || trigger.triggersLeft > 0;
    }

    function isMonitoringTrigger(monitor, triggerName) {
        return monitor && self.monitorStatus.triggerName === triggerName;
    }

    this.initAllTriggers = function () {
        var method = 'initAllTriggers';

        setupFollow('now');

        try{
            logger.info(method, 'resetting system from last state');
            //*********************************************************
            //* Read currently existing trigger configs from DB and 
            //* create a trigger for each 
            //*********************************************************
            self.triggerDB.postView({
                db: self.databaseName,
                ddoc : constants.VIEWS_DESIGN_DOC ,
                view : constants.TRIGGERS_BY_WORKER,
                reduce: false,
                includeDocs: true,
                key: self.worker
            })
            .then(response => {

                if ( response.result) {
                    var err = response.result.error; 
                    var body = response.result.rows; 


                    if ( !err && body ) {
                        body.forEach(function (triggerConfig) {
                            var triggerIdentifier = triggerConfig.id;
                            var doc = triggerConfig.doc;
                            if (!(triggerIdentifier in self.triggers) && !doc.monitor) {
                                //check if trigger still exists in whisk db
                                var namespace = doc.namespace;
                                var name = doc.name;
                                var uri = self.uriHost + '/api/v1/namespaces/' + namespace + '/triggers/' + name;
                                var isIAMNamespace = doc.additionalData && doc.additionalData.iamApikey;

                                logger.info(method, triggerIdentifier, ': Checking if trigger still exists');
                                self.authRequest(doc, {
                                    method: 'get',
                                    uri: uri
                                }, undefined,
                                function (error, response, source ) {
                                    
                                    if (error && source == "auth_handling") {
                                    logger.error(method,  triggerIdentifier, ': Error in handleAuth() request for trigger :', error);
                                    }
                                    
                                    if (!error && shouldDisableTrigger(response.statusCode, response.headers, isIAMNamespace)) {
                                        var message = 'Automatically disabled after receiving a ' + response.statusCode + ' status code on trigger initialization';
                                        disableTrigger(triggerIdentifier, response.statusCode, message);
                                        logger.error(method, triggerIdentifier, ': Trigger has been disabled due to status code :', response.statusCode);
                                    } else {
                                        createTrigger(triggerIdentifier, doc)
                                        .then(cachedTrigger => {
                                            self.triggers[triggerIdentifier] = cachedTrigger;
                                            logger.info(method, triggerIdentifier, ': Created successfully');
                                            if (cachedTrigger.intervalHandle && shouldFireTrigger(cachedTrigger)) {
                                                try {
                                                    fireTrigger(cachedTrigger);
                                                } catch (e) {
                                                    logger.error(method, triggerIdentifier, ': Exception occurred while firing trigger :',  e);
                                                }
                                            }
                                        })
                                        .catch(err => {
                                            var message = 'Automatically disabled after receiving error on trigger initialization: ' + err;
                                            disableTrigger(triggerIdentifier, undefined, message);
                                            logger.error(method,  triggerIdentifier, ': Disabling trigger failed :',err);
                                        });
                                    }
                                });
                            }
                        })    
                    }
                    else {
                        logger.error(method, ': Could not get latest state from database :', err);
                    }
                }
                else {
                    logger.error(method, 'Response from trigger configDB does not contain a result. Only  :', response );
                }    
            })
            .catch( (err) => {
                logger.error(method, "Failed to read  all trigger config info in configDB with  error : ",err);
            })
     
        } catch (err) {
            logger.error(method, ": Error in call command to provider configuration DB : " + err );
        }
    };

    //*********************************************************
    //* setup follow is continuously receiving changes of the  
    //* trigger configDB 
    //* parm: seq - allows to define the start point of changes 
    //*             to receive 
    //*********************************************************
    function setupFollow(seq) {
        var method = 'setupFollow';

        try {
            logger.info(method, "Next trigger configDB read sequence starts on : [", seq, "]");

            //******************************************************************************
            //* use longpoll feed to hold the socket to the trigger configDB open as long as 
            //* possible to reduce HTTP session start/stop cycles . ( max timeout =  1 min)
            //* It may be that one response contains multiple change recods !!! 
            //******************************************************************************
            self.triggerDB.postChanges({ "db" : self.databaseName , "feed" : "longpoll", "timeout" : 60000,  "since": seq , "includeDocs" : true })
            .then(response => {
                //********************************************************************
                //* get the last_seq value to use in the next setupFollow() query 
                //********************************************************************
                var lastSeq = response.result.last_seq ; 
                var numOfChangesDetected = Object.keys(response.result.results).length
                logger.info(method,  numOfChangesDetected + " changes records received from configDB with last seq : ", lastSeq);
               
                for ( i = 0 ; i < numOfChangesDetected; i++ ) {
                    logger.info(method,  "call change Handler with " ,  response.result.results[i]);     
                    changeHandler( response.result.results[i] ); 
                }
                //** Continue to try to read from configDB immediately   
                setupFollow(lastSeq);	
                
            })
            .catch( (err) => {
                logger.error(method, "Failed to read trigger config info in configDB with  error : ",err);
               
                //********************************************************************************
                //* On temporary errors do a immediate retry
                //* On hard errors (e.g coding errors) do a long term retry 
                //********************************************************************************
                var tempErrorCodes   = [ 408, 410, 429, 503, 504 ]; 
                var retryDelay = 3000;  //** 3 seconds delay in case of hard errors 
                
                if ( tempErrorCodes.includes( err.code )) {
                    retryDelay = 100; //** nearly immediate retry 
                }	
                //** Continue to try to read from configDB after a retry wait time  	
                setTimeout( () => {setupFollow( 'now');}, retryDelay );	 
                logger.error(method, ": Error while read on provider configuration DB : " + err , "will retry to read in ", retryDelay, " seconds");
            })
        } catch (err) {
            logger.error(method, ": Error in setting up change listener on provider configuration DB : " + err , "will retry to read in 3 seconds");
            //** Continue to try to read from configDB after a retry wait time   
            setTimeout( () => {setupFollow( 'now');}, 3000 );	
        }
    }

    //***************************************************
    //* react on the event that the trigger configuration
	//* has changed in the trigger configDB 
    //***************************************************
    function changeHandler(change){
        var method = 'changeHandler';

        var triggerIdentifier = change.id;
		var doc = change.doc;
        var triggerDeleted = change.deleted;

        if (self.triggers[triggerIdentifier]) {
            if (doc.status && doc.status.active === false) {
                stopTrigger(triggerIdentifier);
                if (isMonitoringTrigger(doc.monitor, doc.name)) {
                    self.monitorStatus.triggerStopped = "success";
                }
            }
        } else {
            //ignore changes to disabled or deleted triggers
            if ( (!triggerDeleted == true ) && (!doc.status || doc.status.active === true) && (!doc.monitor || doc.monitor === self.host)) {
                createTrigger(triggerIdentifier, doc)
                .then(cachedTrigger => {
                    self.triggers[triggerIdentifier] = cachedTrigger;
                    logger.info(method, triggerIdentifier, ': Created successfully');

                    if (isMonitoringTrigger(cachedTrigger.monitor, cachedTrigger.name)) {
                        self.monitorStatus.triggerStarted = "success";
                    }

                    if (cachedTrigger.intervalHandle && shouldFireTrigger(cachedTrigger)) {
                        try {
                            fireTrigger(cachedTrigger);
                        } catch (e) {
                            logger.error(method,triggerIdentifier, ': Exception occurred while firing trigger :',  e);
                        }
                    }
                })
                .catch(err => {

                    var message = 'Automatically disabled after receiving error on trigger creation: ' + err;
                    disableTrigger(triggerIdentifier, undefined, message);
                    logger.error(method,  triggerIdentifier,': Disabled trigger fail :', err);
                });
            }
        }
	};


    this.authorize = function (req, res, next) {
        var method = 'authorize';

        if (self.endpointAuth) {
            if (!req.headers.authorization) {
                res.set('www-authenticate', 'Basic realm="Private"');
                res.status(HttpStatus.UNAUTHORIZED);
                return res.send('');
            }

            var parts = req.headers.authorization.split(' ');
            if (parts[0].toLowerCase() !== 'basic' || !parts[1]) {
                return sendError(method, HttpStatus.BAD_REQUEST, 'Malformed request, basic authentication expected', res);
            }

            var auth = new Buffer(parts[1], 'base64').toString();
            auth = auth.match(/^([^:]*):(.*)$/);
            if (!auth) {
                return sendError(method, HttpStatus.BAD_REQUEST, 'Malformed request, authentication invalid', res);
            }

            var uuid = auth[1];
            var key = auth[2];
            var endpointAuth = self.endpointAuth.split(':');
            if (endpointAuth[0] === uuid && endpointAuth[1] === key) {
                next();
            } else {
                logger.warn(method, ': Alarms provider detected invalid key');
                return sendError(method, HttpStatus.UNAUTHORIZED, 'Invalid key', res);
            }
        } else {
            next();
        }
    };


    function sendError(method, code, message, res) {
        logger.error(method, message);
        res.status(code).json({error: message});
    }

    this.initRedis = function () {
        var method = 'initRedis';

        return new Promise(function (resolve, reject) {

            if (redisClient) {
                var subscriber = redisClient.duplicate();

                //create a subscriber client that listens for requests to perform swap
                subscriber.on('message', function (channel, message) {
                    logger.info(method, message, 'set to active host in channel', channel);
                    self.activeHost = message;
                });

                subscriber.on('error', function (err) {
                    logger.error(method, 'Error connecting to redis while subscription', err);
                    reject(err);
                });

                subscriber.subscribe(self.redisKey);

                redisClient.hgetAsync(self.redisKey, self.redisField)
                .then(activeHost => {
                	//*********************************************************
                	//* Call a one time sync immediately after init() - 10 sec 
                	//* to fix Sev1 situation when hgetAsync() and subscribe() 
                	//* provide different info 
                	//**********************************************************
                	setTimeout(function () {
                   		logger.info(method, 'Redis one-time synchronizer checks if [ ', self.activeHost, ' ] is still the valid one');
                		redisClient.hgetAsync(self.redisKey, self.redisField)
                        .then(activeHost => {
                        	if ( activeHost != null && activeHost != "" && self.activeHost != activeHost ){
                        		logger.info(method, 'Redis one-time synchronizer updated active host to: ', activeHost);
                        		self.activeHost = activeHost;
                        	}
                         })
                         .catch(err => {
                             logger.error(method, "Redis one-time synchronizer regular run fails with :",  err);
                         })
                     }, 10000 );
                	//************************************************
                	//* Start regularly Redis synchronization, to recover
                	//* from "Redis-Out-of-sync" situations (all 9 min , 
                	//* because default inactivity timeout is 10 min ) 
                	//************************************************
                	setInterval(function () {
                   		logger.info(method, 'Redis synchronizer checks if [ ', self.activeHost, ' ] is still the valid one');
                		redisClient.hgetAsync(self.redisKey, self.redisField)
                        .then(activeHost => {
                        	if ( activeHost != null && activeHost != "" && self.activeHost != activeHost ){
                        		logger.info(method, 'Redis synchronizer updated active host to: ', activeHost);
                        		self.activeHost = activeHost;
                        	}
                         })
                         .catch(err => {
                             logger.error(method, "Redis synchronizer regular run fails with :",  err);
                         })
                     }, 540000 );
                    return initActiveHost(activeHost);
                })
                .then(() => {
                    process.on('SIGTERM', function onSigterm() {
                        if (self.activeHost === self.host) {
                            var redundantHost = self.host === `${self.hostPrefix}0` ? `${self.hostPrefix}1` : `${self.hostPrefix}0`;
                            self.redisClient.hsetAsync(self.redisKey, self.redisField, redundantHost)
                            .then(() => {
                                self.redisClient.publish(self.redisKey, redundantHost);
                           		logger.info(method, 'SIGTERM handler switched active host to: ', redundantHost);
                            })
                            .catch(err => {
                                logger.error(method, "Failed to switch to partner alarm provider while handling SIGTERM, " +err);
                            });    
                        }
                		logger.info(method, 'SIGTERM handler finished ');
                    });
                    resolve();
                })
                .catch(err => {
                	  reject(err);
                });
            } else {
            	logger.info(method, 'Running alarm provider worker without redis connection (test mode) ');
            	resolve();
            }
        });
    };

    function initActiveHost(activeHost) {
        var method = 'initActiveHost';

        if (activeHost === null) {
            //initialize redis key with active host
            logger.info(method, 'redis hset', self.redisKey, self.redisField, self.activeHost);
            return redisClient.hsetAsync(self.redisKey, self.redisField, self.activeHost);
        } else {
            self.activeHost = activeHost;
            logger.info(method, 'start provider with activeHost = ',  self.activeHost);
            return Promise.resolve();
        }
    }

    this.authRequest = function (triggerData, options, body, cb) {

        authHandler.handleAuth(triggerData, options)
        .then(requestOptions => {
            //**********************************************************
        	//* input options must be adapted to match the usage 
        	//* of the needle-package (substitution of request-package)
        	//* Current limitation is, that no query parameters are 
        	//* considered, because providers do not need them. 
        	//**********************************************************
        	var needleMethod = requestOptions.method; 
        	var needleUrl = requestOptions.uri;
        	var needleOptions = {
                rejectUnauthorized: false
            };
            if( requestOptions.auth.user ) {   //* cf-based authorization 
                const usernamePassword = requestOptions.auth.user  +":"+ requestOptions.auth.pass;
                const usernamePasswordEnc = Buffer.from(usernamePassword).toString('base64');
                needleOptions.headers = {}
                needleOptions.headers['Authorization'] = "Basic " + usernamePasswordEnc
            }else if ( requestOptions.auth.bearer) { //* IAM based authorization 
                needleOptions.headers = {}
                needleOptions.headers['Authorization'] = 'Bearer ' +  requestOptions.auth.bearer
            }else {
            	logger.info(method, "no authentication info available");
            }
        
           	if (needleMethod === 'get') {
        		needleOptions.json = false
                needle.request( needleMethod, needleUrl, undefined, needleOptions ,cb);
        	   
        	}else {
        	    needleOptions.json = true
                needleParams = body;
        	    needle.request( needleMethod, needleUrl, needleParams, needleOptions ,cb);
            }
        })
        .catch(err => {
        	//********************************************************************
        	//* added the "source" identifier to enable the callback functions
        	//* to detect if the error happen in handleAuth (IAM or key) or in 
        	//* the http call to openwhisk 
        	//********************************************************************
            cb(err, undefined, "auth_handling");
        });
    };

    function handleFiredTrigger(triggerData) {
        var method = 'handleFiredTrigger';

        if (isMonitoringTrigger(triggerData.monitor, triggerData.name)) {
            self.monitorStatus.triggerFired = "success";
        }

        var triggerIdentifier = triggerData.triggerID;
        if (triggerData.date) {
            if (triggerData.deleteAfterFire && triggerData.deleteAfterFire !== 'false') {

                //delete trigger feed from database
                self.sanitizer.deleteTriggerFeed(triggerIdentifier);

                //check if trigger and all associated rules should be deleted
                if (triggerData.deleteAfterFire === 'rules') {
                    self.sanitizer.deleteTriggerAndRules(triggerData);
                } else {
                    self.sanitizer.deleteTrigger(triggerData, 0)
                    .then(info => {
                        logger.info(method, triggerIdentifier, ":", info);
                    })
                    .catch(err => {
                        logger.error(method, triggerIdentifier, ":", err);
                    });
                }
            } else {
                disableTrigger(triggerIdentifier, undefined, 'Automatically disabled after firing once');
                logger.info(method, triggerIdentifier, ': The fire once date has expired, disabled the trigger ');
            }
        } else if (triggerData.stopDate) {
            //check if the next scheduled trigger is after the stop date
            if (triggerData.cronHandle && triggerData.cronHandle.nextDate().isAfter(new Date(triggerData.stopDate))) {
                disableTrigger(triggerIdentifier, undefined, 'Automatically disabled after firing last scheduled cron trigger');
                logger.info(method, triggerIdentifier,  ': Last scheduled cron trigger before stop date, disabled trigger ');
            } else if (triggerData.minutes && (Date.now() + (triggerData.minutes * 1000 * 60) > new Date(triggerData.stopDate).getTime())) {
                disableTrigger(triggerIdentifier, undefined, 'Automatically disabled after firing last scheduled interval trigger');
                logger.info(method, triggerIdentifier, ': Last scheduled interval trigger before stop date, disabled trigger');
            }
        } else if (triggerData.maxTriggers && triggerData.triggersLeft === 0) {
            disableTrigger(triggerIdentifier, undefined, 'Automatically disabled after reaching max triggers');
            logger.warn(method, triggerIdentifier,  ': No more triggers left, disabled trigger');
        }
    }

};
