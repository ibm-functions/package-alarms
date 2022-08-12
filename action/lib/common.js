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

const needle = require('needle');
const openwhisk = require('openwhisk');
const config = require('./config');

function requestHelper(url, input, method) {
     return new Promise(function (resolve, reject) {
        if (method === 'get') {
            //********************************************************
            //* set json:false to control that needle do not add the 
            //* input parameters to the http request body 
            //********************************************************             
            var options = {
                json : false,
                rejectUnauthorized: false
            };
            url = url + "?" + serialize(input)
            needle.request( 'get', url, undefined, options , function (error, response, body) {
                if (!error && response.statusCode === 200) {
                    resolve(body);
                } else {
                    if (response) {
                        console.log('alarm: Error invoking whisk action (',method,'):', response.statusCode, body);
                        reject(body);
                    } else {
                        console.log('alarm: Error invoking whisk action(',method,'):', error);
                        reject(error);
                    }
                }
            });
        } else {
            //********************************************************
            //* set json:true to control that needle adds the 
            //* input parameters to the http request body 
            //********************************************************             
            var options = {
                json : true,
                rejectUnauthorized: false
            };
            needle.request( method, url, input, options , function (error, response, body) {
                if (!error && response.statusCode === 200) {
                    resolve(body);
                } else {
                    if (response) {
                        console.log('alarm: Error invoking whisk action (', method ,'):', response.statusCode, body);
                        reject(body);
                    } else {
                        console.log('alarm: Error invoking whisk action (', method ,'):', error);
                        reject(error);
                    }
                }
            }); 
        }   
    });
}


//***************************************************************
//* helper function to format parameters to an url-encoded string 
//***************************************************************
serialize = function(obj, prefix) {
  var str = [],
  p;
  for (p in obj) {
    if (obj.hasOwnProperty(p)) {
      var k = prefix ? prefix + "[" + p + "]" : p,
        v = obj[p];
        str.push((v !== null && typeof v === "object") ?
        serialize(v, k) :
        encodeURIComponent(k) + "=" + encodeURIComponent(v));
    }
  }
  return str.join("&");
}


function createWebParams(rawParams) {
    var namespace = process.env.__OW_NAMESPACE;
    var triggerName = '/' + namespace + '/' + parseQName(rawParams.triggerName).name;

    var webparams = Object.assign({}, rawParams);
    delete webparams.lifecycleEvent;
    delete webparams.apihost;

    webparams.triggerName = triggerName;
    webparams.encryptedAuth = process.env.__OW_API_KEY_ENCRYPTED;
    config.addAdditionalData(webparams);

    return webparams;
}

function verifyTriggerAuth(triggerData, isDelete) {
    var owConfig = config.getOpenWhiskConfig(triggerData);
    var ow = openwhisk(owConfig);

    return new Promise(function (resolve, reject) {
    	//**********************************************************************
    	//* check if the Trigger created with a previous 
    	//* call really exist. 
    	//* get() may fail 
    	//*  - if trigger does not exist 
    	//*  - if (in case of IAM namespace) the iam.getAuthHeader() call fails 
    	//***********************************************************************
        ow.triggers.get(triggerData.name)
        .then(() => {
            resolve();
        })
        .catch(err => {
           if (err.statusCode) {
               var statusCode = err.statusCode;
               if (!(isDelete && statusCode === 404)) {
            	   var resultMsg='Check for trigger ('+ triggerData.name + ') in namespace (' + triggerData.namespace + ') failed with status code =' + statusCode + ', msg = '
                   reject(sendError(statusCode, resultMsg, err.message));
               } else {
                   resolve();
               }
           } else {
        	   var resultMsg='Check for trigger ('+ triggerData.name + ') in namespace (' + triggerData.namespace + ') failed with unknown status code, msg = '
               reject(sendError(400, resultMsg, err.message));
           }
        });
    });
}

function parseQName(qname) {
    var parsed = {};
    var delimiter = '/';
    var defaultNamespace = '_';
    if (qname && qname.charAt(0) === delimiter) {
        var parts = qname.split(delimiter);
        parsed.namespace = parts[1];
        parsed.name = parts.length > 2 ? parts.slice(2).join(delimiter) : '';
    } else {
        parsed.namespace = defaultNamespace;
        parsed.name = qname;
    }
    return parsed;
}

function sendError(statusCode, error, message) {
    var params = {error: error};
    if (message) {
        params.message = message;
    }

    return {
        statusCode: statusCode,
        headers: {'Content-Type': 'application/json'},
        body: params
    };
}

function constructObject(data, isPayload) {
    var jsonObject;
    if (data) {
        if (typeof data === 'string') {
            if (isPayload) {
                jsonObject = {payload: data};
            } else {
                try {
                    jsonObject = JSON.parse(data);
                } catch (e) {
                    console.log('error parsing ' + data);
                }
            }
        }
        if (typeof data === 'object') {
            jsonObject = data;
        }
    }
    return jsonObject;
}


module.exports = {
    'requestHelper': requestHelper,
    'createWebParams': createWebParams,
    'verifyTriggerAuth': verifyTriggerAuth,
    'parseQName': parseQName,
    'sendError': sendError,
    'constructObject': constructObject
};
