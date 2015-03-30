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
var Producer = require('./producer');
var Consumer = require('./consumer');

exports.onBootstrap = function(app) {
    jive.logger.info("****************************************");
    jive.logger.info("Sample distributed workqueue processing");
    jive.logger.info("****************************************");

    var error;

    // setup the schema
    (function() {
        var dao = new DataAccessObject();
        return dao.setupSchema();
    })()

    // launch the workers
    .then( function() {
        var role = process.env.__ROLE;

        if ( role === 'producer' ) {
            var producer = new Producer();
            producer.launch();
        }

        if ( role === 'consumer' ) {
            var consumer = new Consumer();
            consumer.launch();
        }
    })
    .catch(function(e) {
        error = e;
    })
    .done(function() {
        if ( error ) {
            throw error;
        }
    });
};