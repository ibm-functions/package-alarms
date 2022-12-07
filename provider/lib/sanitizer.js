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

module.exports = function (logger, manager) {

    var self = this;

    this.deleteTriggerFromDB = function (triggerID, retryCount) {
        var method = 'deleteTriggerFromDB';

        //delete from trigger config database 
       manager.triggerDB.getDocument({
            db: manager.databaseName,
            docId: triggerID
        })
        .then(response => {
            //**************************************************************
            //* if trigger still exist in DB , then remove 
            //**************************************************************    
            manager.triggerDB.deleteDocument({
                db: manager.databaseName,
                docId: triggerID
            })
            .then(response => {
                logger.info(method, triggerID, ': Trigger was successfully deleted from the provider configuration database');
            })
            .catch( (err) => {
                if (err.statusCode === 409 && retryCount < 5) {
                    logger.error(method, triggerID, ": There was an error deleting the trigger from the trigger configuration database with error code = 409, so will retry ");
                    setTimeout(function () {
                        self.deleteTriggerFromDB(triggerID, (retryCount + 1));
                    }, 1000);
                } else {
                    logger.error(method, triggerID, ': There was an error deleting the trigger from the trigger configuration database :', err , " and retry count = ",) ;
                }
            })
        })
        .catch( (err) => {
            logger.error(method, triggerID, ': Could not find the trigger in the database while sanitzer try to delete trigger :', err);
        })
    };

    this.deleteTriggerAndRules = function (triggerData) {
        var method = 'deleteTriggerAndRules';

        var triggerIdentifier = triggerData.triggerID;
        manager.authRequest(triggerData, {
            method: 'get',
            uri: triggerData.uri
        },undefined ,
        function (error, response, body) {
            logger.info(method, triggerIdentifier, ': http get request, STATUS:', response ? response.statusCode : undefined);

            //***************************************************************
            //* in case of error, the body parm contains the source info 
            //**************************************************************
            if ( error && body == "auth_handling") {
                logger.error(method, triggerIdentifier, ': Error in handleAuth() request for trigger :', error);
            }

            if (error ) {
                logger.error(method, triggerIdentifier, ': Trigger get request failed :' , error);
            } else if ( response.statusCode >= 400) {
                logger.error(method, triggerIdentifier, ': Trigger get request failed , with status Code :' , response.statusCode);
            } else {
                //delete the trigger
                self.deleteTrigger(triggerData, 0)
                .then((info) => {
                    logger.info(method, triggerIdentifier, ":", info);
                    if (body) {
                        try {
                            //******************************************************
                            //* Get the rule name (to delete) from the response body
                            //* of the trigger get command 
                            //******************************************************
                            for (var rule in body.rules) {
                                var qualifiedName = rule.split('/');
                                var uri = manager.uriHost + '/api/v1/namespaces/' + qualifiedName[0] + '/rules/' + qualifiedName[1];
                                self.deleteRule(triggerData, rule, uri, 0);
                            }
                        } catch (err) {
                            logger.error(method, triggerIdentifier, ': Failed to delete Rule :', err);
                        }
                    }
                })
                .catch(err => {
                    logger.error(method, triggerIdentifier,': Failed to delete trigger :', err);
                });
            }
        });
    };

    this.deleteTrigger = function (triggerData, retryCount) {
        var method = 'deleteTrigger';

        return new Promise(function (resolve, reject) {

            var triggerIdentifier = triggerData.triggerID;
            var body = {};
            manager.authRequest(triggerData, {
                method: 'delete',
                uri: triggerData.uri
            }, body, 
            function (error, response, source ) {
            
                if ( error && source == "auth_handling") {
                    logger.error(method, triggerIdentifier, ': Error in handleAuth() request for trigger :', error);
                }
                
                logger.info(method, triggerIdentifier, ': http delete request, STATUS :', response ? response.statusCode : undefined );
                if (error || response.statusCode >= 400) {
                    if (!error && response.statusCode === 409 && retryCount < 5) {
                        logger.info(method, triggerIdentifier, ': Attempt to delete trigger again : Retry Count:', (retryCount + 1));
                        setTimeout(function () {
                            self.deleteTrigger(triggerData, (retryCount + 1))
                            .then(info => {
                                resolve(info);
                            })
                            .catch(err => {
                                reject( 'retry of deleteTrigger ', triggerIdentifier,  ' failed with ', err);
                            });
                        }, 1000);
                    } else {
                        reject('trigger ', triggerIdentifier,  'delete request failed');
                    }
                } else {
                    resolve('trigger ', triggerIdentifier, 'delete request was successful');
                }
            });
        });
    };

    this.deleteRule = function (triggerData, rule, uri, retryCount) {
        var method = 'deleteRule';
         
        var body = {}; 
        manager.authRequest(triggerData, {  
            method: 'delete',
            uri: uri
        }, body,
        function (error, response, source ) {
            if ( error && source == "auth_handling") {
               logger.error(method, rule, ': Error in handleAuth() request for trigger :', error);
            }
            logger.info(method, rule, ': http delete rule request for rule finish with STATUS :', response ? response.statusCode : undefined);
            if (error || response.statusCode >= 400) {
                if (!error && response.statusCode === 409 && retryCount < 5) {
                    logger.info(method, rule,': Attempt to delete rule again, Retry Count :', (retryCount + 1));
                    setTimeout(function () {
                        self.deleteRule(triggerData, rule, uri, (retryCount + 1));
                    }, 1000);
                } else {
                    logger.error(method, rule, ':Rule delete request failed after all retries');
                }
            } else {
                logger.info(method, rule, ': Rule delete request was successful');
            }
        });
    };

    this.deleteTriggerFeed = function (triggerID) {
        var method = 'deleteTriggerFeed';

        return new Promise(function (resolve, reject) {

            manager.triggerDB.getDocument({
                db: manager.databaseName,
                docId: triggerID
            })
            .then(response => {
                var updatedTriggerConfig = response.result;
                updatedTriggerConfig.status = {
                    'active': false,
                    'dateChanged': Date.now(),
                    'reason': {'kind': 'AUTO', 'statusCode': undefined, 'message': `Marked for deletion`}
                };

                self.triggerDB.putDocument({
                    db: self.databaseName,
                    docId: triggerID,
                    document: updatedTriggerConfig
                }).then(response => {
                    resolve(triggerID);
                })
                .catch( (err) => {
                    reject('in subcall updateTrigger', err);
                })
            })
            .catch( (err) => {
                reject( 'in subcall getTrigger',err);
            })
        })
        .then(triggerID => {
            self.deleteTriggerFromDB(triggerID, 0);
        })
        .catch(err => {
            logger.error(method, triggerID, ': An error occurred while deleting the trigger feed :', err);
        });
    };

};
