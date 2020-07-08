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

module.exports = function(logger, manager) {

  // Active Endpoint
  this.endPoint = '/active';

  var hostMachine = process.env.HOST_MACHINE;

  this.active = function(req, res) {
      var method = 'active';

      var response = {
          worker: manager.worker,
          host: manager.host,
          hostMachine: hostMachine,
          active: manager.host === manager.activeHost
      };

      if (req.query && req.query.active) {
          var query = req.query.active.toLowerCase();

          if (query !== 'true' && query !== 'false') {
              response.error = "Invalid query string";
              res.send(response);
              return;
          }

          var redundantHost = manager.host === `${manager.hostPrefix}0` ? `${manager.hostPrefix}1` : `${manager.hostPrefix}0`;
          var activeHost = query === 'true' ? manager.host : redundantHost;
          if (manager.activeHost !== activeHost) {
              if (manager.redisClient) {
                  manager.redisClient.hsetAsync(manager.redisKey, manager.redisField, activeHost)
                  .then(() => {
                      response.active = 'swapping';
                      manager.redisClient.publish(manager.redisKey, activeHost);
                      logger.info(method, 'Active host swap in progress');
                      res.send(response);
                  })
                  .catch(err => {
                      response.error = err;
                      res.send(response);
                  });
              }
              else {
                  response.active = manager.host === activeHost;
                  manager.activeHost = activeHost;
                  var message = 'The active state has changed';
                  logger.info(method, message, 'to', activeHost);
                  response.message = message;
                  res.send(response);
              }
          }
          else {
              res.send(response);
          }
      }
      else {
          res.send(response);
      }
  };

};
