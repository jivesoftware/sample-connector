/*
 * Copyright 2013 Jive Software
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

var request = require('request');
var jive = require('jive-sdk');
var spawn = require('child_process').spawn;
var q = require('q');
var child;

//uri for the api-service server - alter as necessary
var servicesUri = 'http://localhost:19400';

//jive endpoint - GET data from here
var jiveEndpoint = '/api/core/v3/contents';

//salesforce endpoint - POST data here
var salesforceEndpoint = '/api/salesforce/receive';

var running = false;

//only check once if it is running
var checked = false;

// temporary in-memory data store - populated by workers ; retrieved by tests
var dataStore = [];


/////////////////////////////////////////////////////////////////////////////////
// Start the test api server as a child_process

exports.start = function () {
    var deferred = q.defer();
    jive.logger.info("starting api-services");

    //path to api-services.jar - alter as necessary
    child = spawn('java', ['-jar', 'test/api-services/api-services-1.0.jar']);

    child.stdout.on('data', function (data) {
        if (data !== null && data.toString().indexOf('Server - Started') > -1) {
            running = true;
            jive.logger.info("api-services running");
            deferred.resolve(running);
        }
        jive.logger.info('server: ' + data);
    });
    child.on('close', function (code) {
        jive.logger.info('closing code: ' + code);
    });
    return deferred.promise;
};


// stop the api server
exports.stop = function () {
    var deferred = q.defer();
    child.kill();
    deferred.resolve();
    return deferred.promise;
};

// call the api and retrieve a data point
exports.generate = function (numData) {
    var deferred = q.defer();
    request({
            url: servicesUri + jiveEndpoint + "?count=" + numData,
            method: "GET"
        },
        function (err, res, body) {
            if (!err && res.statusCode === 200) {
                deferred.resolve(body);
            }
            else {
                deferred.reject(err);
            }
        });
    return deferred.promise;
};


// call the api and post a data point
exports.send = function (data) {
    var deferred = q.defer();
    request({
            url: servicesUri + salesforceEndpoint,
            method: "POST",
            json: true,
            headers: {
                "content-type": "application/json"
            },
            body: data
        },
        function (err, res, body) {
            if (!err && res.statusCode === 200) {
                deferred.resolve(body);
            }
            else {
                deferred.reject(new Error("Response other than 200 from api"));
            }
        });
    return deferred.promise;
};

// add to the temporary data store - used by workers
exports.store = function (data) {
    var deferred = q.defer();
    dataStore.push(data);
    deferred.resolve();
    return deferred.promise;
};

// retrieve from the data store - used by tests
exports.retrieve = function () {
    var deferred = q.defer();
    if (dataStore.length > 0) {
        deferred.resolve(dataStore.pop());
    }
    return deferred.promise;
};

// poll for data - once a worker has populated the data store, return
exports.waitForData = function() {
    var deferred = q.defer();
    jive.logger.info("Begin polling for deliverable data");
    var poll = setInterval(function() {
        if (dataStore.length > 0) {
            jive.logger.info("Data is ready to be delivered");
            deferred.resolve();
            clearInterval(poll);
        }
    }, 1000);
    return deferred.promise;
};

// clear the data store
exports.clearStore = function () {
    var deferred = q.defer();
    dataStore = [];
    deferred.resolve();
    return deferred.promise;
};

//Determine if the api is running - only check once
exports.isRunning = function () {
    var deferred = q.defer();
    if (!checked && !running) {
        exports.generate(1).then(function () {
            running = true;
            deferred.resolve(running);
        }, function () {
            running = false;
            deferred.reject(running);
        });
        checked = true;
    }
    else {
        if (running) {
            deferred.resolve(running);
        }
        else {
            deferred.reject(running);
        }
    }
    return deferred.promise;
};

