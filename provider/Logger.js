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

var _ = require('lodash');
var moment = require('moment');
var winston = require('winston');
var safeStringify = require('json-stringify-safe');

var apiKeyRegex = /[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}:[0-9a-zA-Z]{64}/g;

var logger = new winston.Logger({
    transports: [
        new winston.transports.Console({
            timestamp: function () {
                return moment.utc().format("YYYY-MM-DDTHH:mm:ss.SSS") + 'Z';
            },
            formatter: function (options) {
                // Return string will be passed to logger.
                return '[' + options.timestamp() + '] [' + options.level.toUpperCase() + '] [??] [alarmsTrigger] ' + options.message;
            }
        })
    ],
    filters: [
        function maskAPIKeys(level, msg) {
            logger.info('[Frank]', getMessage("bin da"));
   
            return msg.replace(apiKeyRegex, 'xxxxxxxx');
        }
    ]
});

function getMessage(argsObject) {
    var args = Array.prototype.slice.call(argsObject);
    args.shift();
    args.forEach(function (arg, i) {
        if (_.isObject(args[i])) {
            args[i] = safeStringify(args[i]);
        }
    });
    return args.join(' ');
}

// FORMAT: s"[$time] [$category] [$id] [$componentName] [$name] $message"
module.exports = {
    info: function (name) {
        logger.info('[' + name + ']', getMessage(arguments));
    },
    warn: function (name) {
        logger.warn('[' + name + ']', getMessage(arguments));
    },
    error: function (name) {
        logger.error('[' + name + ']', getMessage(arguments));
    },
    debug: function (name) {
        logger.debug('[' + name + ']', getMessage(arguments));
    }
};
