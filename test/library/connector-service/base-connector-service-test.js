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

var q = require('q');
var jive = require('jive-sdk');
var apiService = require('../../api-services/api-services');
var validator = require('./validator');

// Simple Smoke Test
exports.testConnectorService = function() {
    var deferred = q.defer();

    // poll until a worker has populated data in the data store
    apiService.waitForData()

        // retrieve the deliverable data
        .then(function() { return apiService.retrieve(); })

        // attempt to send the data to the server for validation
        .then(function (data) { return apiService.send(data); }, function(err) { deferred.reject(err); })

        // send the response through the validator to ensure a 'success=true' was returned from the api
        .then(function (data) { return validator.validate(data); }, function(err) { deferred.reject(err); })

        // establish our promise on validator pass/fail
        .then (function () { return deferred.resolve();}, function(err) { deferred.reject(err); });

    return deferred.promise;
};
