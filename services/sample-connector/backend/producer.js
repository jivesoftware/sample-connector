/*
 * Copyright 2015 Jive Software
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

var jive = require('jive-sdk');
var q = require('q');
var DataAccessObject = require('./dao');
var apiServices = require('../../../test/api-services/api-services');

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Public API

/**
 * This entity is responsible for populating the workitems table with items to be processed by workers.
 * @constructor
 */
function Producer() {
    jive.logger.info("******************************");
    jive.logger.info("* producer *");
    jive.logger.info("******************************");

    this.dao = new DataAccessObject();
}

module.exports = Producer;

var useTestApiService = function () {
    return apiServices.generate(1).then(
        //data successfully returned from api
        function (data) {
            var deferred = q.defer();
            deferred.resolve(data);
            return deferred.promise;
        },

        //error returned from api
        function (err) {
            jive.logger.info("ERROR:" + err);
        })
};

var generateGuid = function () {
    var deferred = q.defer();
    deferred.resolve(jive.util.guid());
    return deferred.promise;
};

Producer.prototype.launch = function () {
    var self = this;

    var produceWorkItem = function () {
        var workOwnerID = getRandomInt(1, 5);
        var modificationTime = new Date().getTime();

        //Determine if the api-service is running
        // if running: use it to generate data
        // if not: generate a guid

        apiServices.isRunning()
            .then(useTestApiService, generateGuid)
            .then(function (payload) {
                self.dao.addWork(workOwnerID, payload, modificationTime)
                    .then(function () {
                        jive.logger.info("Inserted workOwnerID " + workOwnerID + ", payload " + payload);
                    });
            });
    };
    // schedule
    var producer_rate = process.env.__PRODUCER_RATE || 250;
    var id = jive.util.guid();
    var task = jive.tasks.build(produceWorkItem, producer_rate, id);
    jive.tasks.schedule( task, jive.service.scheduler());
};

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Private

function getRandomInt(min, max) {
    return Math.floor(Math.random() * (max - min + 1)) + min;
}

