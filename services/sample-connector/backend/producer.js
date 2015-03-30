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

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Public API

/**
 * This entity is responsible for populating the workqueue table with items to be processed by consumers.
 * @constructor
 */
function Producer() {
    jive.logger.info("******************************");
    jive.logger.info("* producer *");
    jive.logger.info("******************************");

    this.dao = new DataAccessObject();
}

module.exports = Producer;

Producer.prototype.launch = function() {
    var self = this;

    var produceWorkItem = function () {
        var ownerID = getRandomInt(1, 5);
        var payload = jive.util.guid();
        var modificationTime = new Date().getTime();

        self.dao.addWork(ownerID, payload, modificationTime).then(function() {
            jive.logger.info("Inserted ownerID " + ownerID + ", payload " + payload);
        });
    };

    // schedule
    var producer_rate = process.env.__PRODUCER_RATE || 10;
    var id = jive.util.guid();
    var task = jive.tasks.build(produceWorkItem, producer_rate, id);
    jive.tasks.schedule( task, jive.service.scheduler());
};

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Private

function getRandomInt(min, max) {
    return Math.floor(Math.random() * (max - min + 1)) + min;
}

