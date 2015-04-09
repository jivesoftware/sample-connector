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
var Producer = require('../services/sample-connector/backend/producer');
var Worker = require('../services/sample-connector/backend/worker');
var express = require('express');
var http = require('http');

var app = express();

// fail if unable to start service
var failServer = function (reason) {
    console.log('FATAL -', reason);
    process.exit(-1);
};

//start express server
var startServer = function () {
    var deferred = q.defer();
    if (!jive.service.role || jive.service.role.isHttp()) {
        var server = http.createServer(app).listen(app.get('port') || 8090, app.get('hostname') || undefined, function () {
            console.log("Express server listening on " + server.address().address + ':' + server.address().port);
        });
    }
    deferred.resolve();
    return deferred;
};

// wire up and start the jive services
exports.start = function () {
    var deferred = q.defer();
    jive.service.init(app)
        .then( function() { return jive.service.autowire() } )
        .then( function() { return jive.service.start() } )
        .then( startServer, failServer )
        .then (function() {
            deferred.resolve();
        });

    return deferred.promise;
};

// stop the jive services
exports.stop = function () {
    var deferred = q.defer();
    jive.service.stop();
    deferred.resolve();
    return deferred.promise;
};

// start a test worker (establish the test context)
exports.startTestWorker = function () {
    var deferred = q.defer();
    var worker = new Worker();
    worker.establishTestContext();
    worker.launch();
    deferred.resolve(worker);
    return deferred.promise;
};

// start a test producer
exports.startTestProducer = function () {
    var deferred = q.defer();
    var producer = new Producer();
    producer.launch();
    deferred.resolve(producer);
    return deferred.promise;
};
