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
var Worker = require('./worker');

exports.onBootstrap = function(app) {
    jive.logger.info("****************************************");
    jive.logger.info("Sample distributed work item processing");
    jive.logger.info("****************************************");

    var error;

    // setup the schema
    return new DataAccessObject().setupSchema()

    // launch the workers
    .then( function() {
        var role = process.env.__ROLE;

        if ( role === 'producer' || role == 'all' ) {
            var producer = new Producer();
            producer.launch();
        }

        if ( role === 'worker' || role == 'all' ) {
            var numWorkers = process.env.__NUM_WORKERS || 1;
            for ( var i = 0; i < numWorkers; i++ ) {
                var worker = new Worker();
                worker.launch();
            }
        }
    })
    .catch(function(e) {
        // fail w/ an error, so it can be thrown by the downstream fail handler
        // note - for some reason a throw here doesn't actually do anything
        return q.reject(e);
    }).fail(function(r) {
        throw r;
    });
};